import yaml
from classes import pcdm
from classes.exceptions import RESTAPIException, FailureException
import logging
logger = logging.getLogger(__name__)

class Command:
    def __init__(self, subparsers):
        parser_mkcol = subparsers.add_parser('mkcol',
                description='Create a PCDM Collection in the repository')
        parser_mkcol.add_argument('-n', '--name',
                            help='Name of the collection.',
                            action='store',
                            required=True
                            )
        # if given, will write the collection URI to it
        parser_mkcol.add_argument('-b', '--batch',
                            help='Path to batch configuration file.',
                            action='store'
                            )
        parser_mkcol.set_defaults(cmd_name='mkcol')

    def __call__(self, fcrepo, args):
        # open transaction
        logger.info('Opening transaction')
        fcrepo.open_transaction()

        try:
            collection = pcdm.Collection()
            collection.title = args.name
            collection.create_object(fcrepo)
            collection.update_object(fcrepo)
            # commit transaction
            logger.info('Committing transaction')
            fcrepo.commit_transaction()

        except (RESTAPIException) as e:
            logger.error("Error in collection creation: {0}".format(e))
            raise FailureException()

        if args.batch is not None:
            with open(args.batch, 'r') as batchconfig:
                batch = yaml.safe_load(batchconfig)
                batch['COLLECTION'] = str(collection.uri)
            with open(args.batch, 'w') as batchconfig:
                yaml.dump(batch, batchconfig, default_flow_style=False)
