import yaml
from classes import pcdm
from classes.exceptions import RESTAPIException
import logging
logger = logging.getLogger(__name__)

def run(fcrepo, args):
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
        # failures here will be caught by the main loop's exception handler
        # and should trigger a system exit
        logger.error("Error in collection creation: {0}".format(e))

    if args.batch is not None:
        with open(args.batch, 'r') as batchconfig:
            batch = yaml.safe_load(batchconfig)
            batch['COLLECTION'] = str(collection.uri)
        with open(args.batch, 'w') as batchconfig:
            yaml.dump(batch, batchconfig, default_flow_style=False)
