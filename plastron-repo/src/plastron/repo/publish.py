import logging

from plastron.handles import HandleBearingResource, HandleServiceClient, HandleServerError, parse_handle_string, \
    HandleInfo
from plastron.namespaces import umdaccess
from plastron.rdfmapping.resources import RDFResource
from plastron.repo import RepositoryResource, RepositoryError

logger = logging.getLogger(__name__)


def get_publication_status(obj: RDFResource) -> str:
    if umdaccess.Published in obj.rdf_type.values:
        if umdaccess.Hidden in obj.rdf_type.values:
            return 'PublishedHidden'
        else:
            return 'Published'
    else:
        if umdaccess.Hidden in obj.rdf_type.values:
            return 'UnpublishedHidden'
        else:
            return 'Unpublished'


class PublishableResource(RepositoryResource):
    @property
    def publication_status(self):
        return get_publication_status(self.describe(RDFResource))

    def publish(
            self,
            handle_client: HandleServiceClient,
            public_url: str,
            force_hidden: bool = False,
            force_visible: bool = False,
    ) -> HandleInfo:
        obj = self.describe(HandleBearingResource)

        # find or create handle
        if obj.has_handle:
            logger.debug(f'Handle in the repository: {obj.handle.value}')

            # check the handle service to see if there is a registered handle
            # if so, check the URL and update it to the fcrepo url if need be
            handle_info = handle_client.get_info(*parse_handle_string(obj.handle.value))
            if not handle_info.exists:
                logger.error(f'Unable to find expected handle {obj.handle.value} for {self.url} on the handle server')
                raise RepositoryError(f'Unable to publish {self}')

            logger.debug(f'Handle service resolves {obj.handle.value} to {handle_info.url}')

            # check target URL, and update if needed
            updates = {}
            if handle_info.url != public_url:
                logger.warning(f'Current target URL ({handle_info.url}) does not match the expected URL ({public_url})')
                updates['url'] = public_url

            if handle_info.repo != handle_client.default_repo:
                logger.warning(
                    f'Current repo ({handle_info.repo}) does not match '
                    f'the expected value "{handle_client.default_repo}"'
                )
                updates['repo'] = handle_client.default_repo

            if handle_info.repo_id != self.url:
                logger.warning(f'Current repo id ({handle_info.repo_id}) does not match the expected URL ({self.url})')
                updates['repo_id'] = self.url

            if updates:
                handle_info = handle_client.update_handle(handle_info, **updates)
                logger.info(f'Updated {handle_info} with {updates}')
        else:
            handle_info = handle_client.find_handle(repo_id=self.url)
            if not handle_info.exists:
                # create a new handle
                logger.debug(f'Minting new handle for {self.url}')
                try:
                    handle_info = handle_client.create_handle(
                        repo_id=self.url,
                        url=public_url,
                    )
                except HandleServerError as e:
                    # if the handle is still not created, something *really* went wrong
                    logger.error(f'Unable to find or create handle for {self.url}: {e}')
                    raise RepositoryError(f'Unable to publish {self}') from e

            # set the object's handle
            obj.handle = handle_info.hdl_uri

        # add the Published (and optionally, add or remove the Hidden) access classes
        obj.rdf_type.add(umdaccess.Published)
        if force_hidden:
            obj.rdf_type.add(umdaccess.Hidden)
        elif force_visible:
            obj.rdf_type.remove(umdaccess.Hidden)

        # save changes
        self.update()

        return handle_info

    def unpublish(self, force_hidden: bool = False, force_visible: bool = False):
        obj = self.describe(RDFResource)

        # remove the Published (and optionally, add or remove the Hidden) access classes
        obj.rdf_type.remove(umdaccess.Published)
        if force_hidden:
            obj.rdf_type.add(umdaccess.Hidden)
        elif force_visible:
            obj.rdf_type.remove(umdaccess.Hidden)

        # save changes
        self.update()
