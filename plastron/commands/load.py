import logging.config
import os
import re
import yaml
from argparse import ArgumentTypeError
from datetime import datetime
from importlib import import_module
from time import sleep
from plastron.exceptions import ConfigException, DataReadException, RESTAPIException, FailureException
from plastron.http import Transaction
from plastron.util import print_header, print_footer, ItemLog

logger = logging.getLogger(__name__)
now = datetime.utcnow().strftime('%Y%m%d%H%M%S')

class Command:
    def __init__(self, subparsers):
        parser_load = subparsers.add_parser('load',
                description='Load a batch into the repository')
        required = parser_load.add_argument_group('required arguments')
        required.add_argument('-b', '--batch',
                            help='path to batch configuration file',
                            action='store',
                            required=True
                            )
        parser_load.add_argument('-d', '--dryrun',
                            help='iterate over the batch without POSTing',
                            action='store_true'
                            )
        # useful for testing when file loading is too slow
        parser_load.add_argument('-n', '--nobinaries',
                            help='iterate without uploading binaries',
                            action='store_true'
                            )
        parser_load.add_argument('-l', '--limit',
                            help='limit the load to a specified number of top-level objects',
                            action='store',
                            type=int,
                            default=None
                            )
        # load an evenly-spaced percentage of the total batch
        parser_load.add_argument('-%', '--percent',
                            help='load specified percentage of total items',
                            action='store',
                            type=percentage,
                            default=None
                            )
        parser_load.add_argument('--noannotations',
                            help='iterate without loading annotations (e.g. OCR)',
                            action='store_true'
                            )
        parser_load.add_argument('--ignore', '-i',
                            help='file listing items to ignore',
                            action='store'
                            )
        parser_load.add_argument('--wait', '-w',
                            help='wait n seconds between items',
                            action='store'
                            )
        parser_load.set_defaults(cmd_name='load')

    def __call__(self, fcrepo, args):
        if not args.quiet:
            print_header()

        # Load batch configuration
        try:
            batch_config = BatchConfig(args.batch)
        except ConfigException as e:
            logger.error(e.message)
            logger.error(f'Failed to load batch configuration from {args.batch}')
            raise FailureException(e.message)

        logger.info(f'Loaded batch configuration from {args.batch}')

        if not os.path.isdir(batch_config.log_dir):
            os.makedirs(batch_config.log_dir)

        if args.nobinaries:
            fcrepo.load_binaries = False

        # Define the data_handler function for the data being loaded
        logger.info("Initializing data handler")
        module_name = batch_config.handler
        handler = import_module('plastron.handlers.' + module_name)
        logger.info('Loaded "{0}" handler'.format(module_name))

        # "--nobinaries" implies "--noannotations"
        if args.nobinaries:
            logger.info("Setting --nobinaries implies --noannotations")
            args.noannotations = True

        try:
            batch = handler.Batch(fcrepo, batch_config)
        except (ConfigException, DataReadException) as e:
            logger.error(e.message)
            logger.error('Failed to initialize batch')
            raise FailureException(e.message)

        if not args.dryrun:
            fcrepo.test_connection()

            # read the log of completed items
            fieldnames = ['number', 'timestamp', 'title', 'path', 'uri']
            try:
                completed = ItemLog(batch_config.mapfile, fieldnames, 'path')
            except Exception as e:
                logger.error(f"Non-standard map file specified: {e}")
                raise FailureException()

            logger.info(f"Found {len(completed)} completed items")

            if args.ignore is not None:
                try:
                    ignored = ItemLog(args.ignore, fieldnames, 'path')
                except Exception as e:
                    logger.error(f"Non-standard ignore file specified: {e}")
                    raise FailureException()
            else:
                ignored = []

            skipfile = os.path.join(
                batch_config.log_dir, 'skipped.load.{0}.csv'.format(now)
                )
            skipped = ItemLog(skipfile, fieldnames, 'path')

            load_set = get_load_set(batch, args.percent)

            # create all batch objects in repository
            for n, item in enumerate(batch):
                is_loaded = False

                if n not in load_set:
                    logger.info(f"Loading {args.percent}, skipping item {n}")
                    continue

                # handle load limit parameter
                if args.limit is not None and n >= args.limit:
                    logger.info(f"Stopping after {args.limit} item(s)")
                    break
                elif item.path in completed:
                    continue
                elif item.path in ignored:
                    logger.debug(f"Ignoring {item.path}")
                    continue

                logger.info(f"Processing item {n + 1}/{batch.length}...")
                if args.verbose:
                    item.print_item_tree()

                try:
                    logger.info(f"Loading item {n+1}")
                    is_loaded = load_item(
                        fcrepo, item, args, extra=batch_config.extra
                        )
                except RESTAPIException:
                    logger.error(
                        "Unable to commit or rollback transaction, aborting"
                        )
                    raise FailureException()
                except DataReadException as e:
                    logger.error(f"Skipping item {n + 1}: {e.message}")

                row = {'number': n + 1,
                       'path': item.path,
                       'timestamp': getattr(
                            item, 'creation_timestamp', str(datetime.utcnow())
                            ),
                       'title': getattr(item, 'title', 'N/A'),
                       'uri': getattr(item, 'uri', 'N/A')
                       }

                # write item details to relevant summary CSV
                if is_loaded:
                    completed.writerow(row)
                else:
                    skipped.writerow(row)

                if args.wait:
                    logger.info("Pausing {0} seconds".format(args.wait))
                    sleep(int(args.wait))

        if not args.quiet:
            print_footer()

def get_load_set(batch, percent=None):
    """set up interval from percent parameter and store set of items to load"""
    if percent is None:
        percent = 100
    indexes = list(range(0, batch.length, int(100/percent)))
    if percent < 100:
        logger.info(f"Items to load: {', '.join(indexes)}")
    else:
        logger.info("Loading all items")
    return set(indexes)

# custom argument type for percentage loads
def percentage(n):
    p = int(n)
    if not p > 0 and p < 100:
        raise ArgumentTypeError("Percent param must be 1-99")
    return p

def load_item(fcrepo, batch_item, args, extra=None):
    # read data for item
    logger.info('Reading item data')
    item = batch_item.read_data()

    # open transaction
    with Transaction(fcrepo, keep_alive=90) as txn:
        # create item and its components
        try:
            logger.info('Creating item')
            item.recursive_create(fcrepo)
            logger.info('Creating ordering proxies')
            item.create_ordering(fcrepo)
            if not args.noannotations:
                logger.info('Creating annotations')
                item.create_annotations(fcrepo)

            if extra:
                logger.info('Adding additional triples')
                if re.search(r'\.(ttl|n3|nt)$', extra):
                    rdf_format = 'n3'
                elif re.search(r'\.(rdf|xml)$', extra):
                    rdf_format = 'xml'
                else:
                    raise ConfigException("Unrecognized extra triples file format")
                item.add_extra_properties(extra, rdf_format)

            logger.info('Updating item and components')
            item.recursive_update(fcrepo)
            if not args.noannotations:
                logger.info('Updating annotations')
                item.update_annotations(fcrepo)

            # commit transaction
            txn.commit()
            logger.info('Performing post-creation actions')
            item.post_creation_hook()
            return True

        except (RESTAPIException, FileNotFoundError) as e:
            # if anything fails during item creation or committing the transaction
            # attempt to rollback the current transaction
            # failures here will be caught by the main loop's exception handler
            # and should trigger a system exit
            logger.error("Item creation failed: {0}".format(e))
            txn.rollback()
            logger.warning('Transaction rolled back. Continuing load.')

        except KeyboardInterrupt as e:
            logger.error("Load interrupted")
            raise e

class BatchConfig:
    def __init__(self, filename):
        self.filename = filename
        with open(filename, 'r') as config_file:
            options = yaml.safe_load(config_file)

        # root_dir defaults to the same directory as the config file
        self.root_dir = options.get('ROOT_DIR', os.path.dirname(self.filename))
        # data_dir defaults to <root_dir>/data
        self.data_dir = os.path.join(self.root_dir, options.get('DATA_DIR', 'data'))
        # log_dir defaults to <root_dir>/logs
        self.log_dir = os.path.join(self.root_dir, options.get('LOG_DIR', 'logs'))
        # mapfile defaults to <log_dir>/mapfile.csv
        self.mapfile = os.path.join(self.log_dir, options.get('MAPFILE', 'mapfile.csv'))

        self.handler_options = options.get('HANDLER_OPTIONS', {})
        self.extra = options.get('EXTRA', None)

        # required fields
        missing_fields = []
        try:
            self.batch_file = os.path.join(self.data_dir, options['BATCH_FILE'])
        except KeyError:
            missing_fields.append('BATCH_FILE')
        try:
            self.collection_uri = options['COLLECTION']
        except KeyError:
            missing_fields.append('COLLECTION')
        try:
            self.handler = options['HANDLER']
        except KeyError:
            missing_fields.append('HANDLER')

        if missing_fields:
            raise ConfigException('Missing required batch configuration field(s): '
                    + ', '.join(missing_fields))
