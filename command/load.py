import csv
from fractions import gcd
from importlib import import_module
import os.path
import sys
import yaml
import re
import logging
import logging.config
from datetime import datetime
from classes import pcdm,util
from classes.exceptions import ConfigException, DataReadException, RESTAPIException
from time import sleep
import namespaces
import logging
from classes.util import print_header, print_footer

logger = logging.getLogger(__name__)
now = datetime.utcnow().strftime('%Y%m%d%H%M%S')

def run(fcrepo, args):
    if not args.quiet:
        print_header()

    # Load batch configuration
    with open(args.batch, 'r') as batch_config:
        batch_options = yaml.safe_load(batch_config)
        log_location = batch_options.get('LOG_LOCATION')

    logger.info('Loaded batch configuration from {0}'.format(args.batch))

    if args.nobinaries:
        fcrepo.load_binaries = False

    # Define the data_handler function for the data being loaded
    logger.info("Initializing data handler")
    module_name = batch_options.get('HANDLER')
    handler = import_module('handler.' + module_name)
    logger.info('Loaded "{0}" handler'.format(module_name))

    # "--nobinaries" implies "--noannotations"
    if args.nobinaries:
        logger.info("Setting --nobinaries implies --noannotations")
        args.noannotations = True

    # Invoke the data handler by calling the load function on the batch config
    try:
        batch = handler.load(fcrepo, batch_options)
    except ConfigException as e:
        logger.error(e.message)
        logger.error(
            "Failed to load batch configuration from {0}".format(args.batch)
            )
        sys.exit(1)

    if not args.dryrun:
        fcrepo.test_connection()

        # read the log of completed items
        mapfile = os.path.join(log_location, batch_options.get('MAPFILE'))
        fieldnames = ['number', 'timestamp', 'title', 'path', 'uri']
        try:
            completed = util.ItemLog(mapfile, fieldnames, 'path')
        except Exception as e:
            logger.error('Non-standard map file specified: {0}'.format(e))
            sys.exit(1)

        logger.info('Found {0} completed items'.format(len(completed)))

        if args.ignore is not None:
            try:
                ignored = util.ItemLog(args.ignore, fieldnames, 'path')
            except Exception as e:
                logger.error('Non-standard ignore file specified: {0}'.format(e))
                sys.exit(1)
        else:
            ignored = []

        skipfile = os.path.join(log_location, 'skipped.load.{0}.csv'.format(now))
        skipped = util.ItemLog(skipfile, fieldnames, 'path')

        # set up interval from percent parameter and store set of items to load
        if args.percent is not None:
            gr_common_div = gcd(100, args.percent)
            denom = int(100 / gr_common_div)
            numer = int(args.percent / gr_common_div)
            logger.info('Loading {0} of every {1} items (= {2}%)'.format(
                            numer, denom, args.percent
                            ))
            load_set = set()
            for i in range(0, batch.length, denom):
                load_set.update(range(i, i + numer))
            logger.info(
                'Items to load: {0}'.format(
                    ', '.join([str(s + 1) for s in sorted(load_set)])
                    ))

        # create all batch objects in repository
        for n, item in enumerate(batch):
            is_loaded = False

            if args.percent is not None and n not in load_set:
                logger.info(
                    'Loading {0} percent, skipping {1}'.format(args.percent, n)
                    )
                continue

            # handle load limit parameter
            if args.limit is not None and n >= args.limit:
                logger.info("Stopping after {0} item(s)".format(args.limit))
                break
            elif item.path in completed:
                continue
            elif item.path in ignored:
                logger.debug('Ignoring {0}'.format(item.path))
                continue

            logger.info(
                "Processing item {0}/{1}...".format(n+1, batch.length)
                )
            if args.verbose:
                item.print_item_tree()

            try:
                logger.info('Loading item {0}'.format(n+1))
                is_loaded = load_item(
                    fcrepo, item, args, extra=batch_options.get('EXTRA')
                    )
            except RESTAPIException as e:
                logger.error(
                    "Unable to commit or rollback transaction, aborting"
                    )
                sys.exit(1)
            except DataReadException as e:
                logger.error(
                    "Skipping item {0}: {1}".format(n + 1, e.message)
                    )

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

def load_item(fcrepo, item, args, extra=None):
    # read data for item
    logger.info('Reading item data')
    item.read_data()

    # open transaction
    logger.info('Opening transaction')
    fcrepo.open_transaction()

    # create item and its components
    try:
        keep_alive = pcdm.TransactionKeepAlive(fcrepo, 90)
        keep_alive.start()

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
            item.add_extra_properties(extra, rdf_format)

        logger.info('Updating item and components')
        item.recursive_update(fcrepo)
        if not args.noannotations:
            logger.info('Updating annotations')
            item.update_annotations(fcrepo)

        keep_alive.stop()

        # commit transaction
        logger.info('Committing transaction')
        fcrepo.commit_transaction()
        logger.info('Performing post-creation actions')
        item.post_creation_hook()
        return True

    except (RESTAPIException, FileNotFoundError) as e:
        # if anything fails during item creation or commiting the transaction
        # attempt to rollback the current transaction
        # failures here will be caught by the main loop's exception handler
        # and should trigger a system exit
        logger.error("Item creation failed: {0}".format(e))
        fcrepo.rollback_transaction()
        logger.warn('Transaction rolled back. Continuing load.')

    except KeyboardInterrupt as e:
        # set the stop flag on the keep-alive ping
        keep_alive.stop()
        logger.error("Load interrupted")
        sys.exit(2)
