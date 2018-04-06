from classes.exceptions import RESTAPIException, FailureException
from classes.util import print_header, print_footer, parse_predicate_list
import logging

logger = logging.getLogger(__name__)

def run(fcrepo, args):
    if not args.quiet:
        print_header()

    if args.recursive is not None:
        logger.info('Recursive delete enabled')
        args.predicates = parse_predicate_list(args.recursive)
        logger.info('Deletion will traverse the following predicates: {0}'.format(
            ', '.join([ p.n3() for p in args.predicates ]))
            )

    test_connection(fcrepo)
    if args.dryrun:
        logger.info('Dry run enabled, no actual deletions will take place')

    try:
        if args.file is not None:
            with open(args.file, 'r') as uri_list:
                delete_items(fcrepo, uri_list, args)
        elif args.uris is not None:
            delete_items(fcrepo, args.uris, args)

    except RESTAPIException as e:
        logger.error(
            "Unable to commit or rollback transaction, aborting"
            )
        raise FailureException()

    if not args.quiet:
        print_footer()

def get_uris_to_delete(fcrepo, uri, args):
    if args.recursive is not None:
        logger.info('Constructing list of URIs to delete')
        return fcrepo.recursive_get(uri, traverse=args.predicates)
    else:
        return fcrepo.recursive_get(uri, traverse=[])

def delete_items(fcrepo, uri_list, args):
    if args.dryrun:
        for uri in uri_list:
            for (target_uri, graph) in get_uris_to_delete(fcrepo, uri, args):
                title = get_title_string(graph)
                logger.info("Would delete {0} {1}".format(target_uri, title))
        return True

    # open transaction
    logger.info('Opening transaction')
    fcrepo.open_transaction()

    # delete item
    # (and its components, if a list of predicates to traverse was given)
    try:
        for uri in uri_list:
            for (target_uri, graph) in get_uris_to_delete(fcrepo, uri, args):
                title = get_title_string(graph)
                fcrepo.delete(target_uri)
                logger.info('Deleted resource {0} {1}'.format(target_uri, title))

        # commit transaction
        logger.info('Committing transaction')
        fcrepo.commit_transaction()
        return True

    except RESTAPIException as e:
        # if anything fails during deletion of a set of uris, attempt to
        # rollback the transaction. Failures here will be caught by the main
        # loop's exception handler and should trigger a system exit
        logger.error("Item deletion failed: {0}".format(e))
        fcrepo.rollback_transaction()
        logger.warn('Transaction rolled back.')
