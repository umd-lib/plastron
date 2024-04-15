import logging

from plastron.handles import HandleBearingResource, Handle, HandleServiceClient, HandleServerError
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
    ) -> Handle:
        obj = self.describe(HandleBearingResource)

        # find or create handle
        if obj.has_handle:
            logger.debug(f'Handle in the repository: {obj.handle.value}')

            # check the handle service to see if there is a registered handle
            # if so, check the URL and update it to the fcrepo url if need be
            handle = handle_client.resolve(Handle.parse(obj.handle.value))
            if handle is None:
                logger.error(f'Unable to find expected handle {obj.handle.value} for {self.url} on the handle server')
                raise RepositoryError(f'Unable to publish {self}')

            logger.debug(f'Handle service resolves {handle.hdl_uri} to {handle.url}')

            # check target URL, and update if needed
            if handle.url != public_url:
                logger.warning(f'Current target URL ({handle.url}) does not match the expected URL ({public_url})')
                handle = handle_client.update_handle(handle, url=public_url)
                logger.info(f'Updated {handle.hdl_uri} target URL to {public_url}')
        else:
            handle = handle_client.find_handle(repo_uri=self.url)
            if handle is None:
                # create a new handle
                logger.debug(f'Minting new handle for {self.url}')
                try:
                    handle = handle_client.create_handle(
                        repo_uri=self.url,
                        url=public_url,
                    )
                except HandleServerError as e:
                    # if the handle is still not created, something *really* went wrong
                    logger.error(f'Unable to find or create handle for {self.url}: {e}')
                    raise RepositoryError(f'Unable to publish {self}') from e

            # set the object's handle
            obj.handle = handle.hdl_uri

        # add the Published (and optionally, add or remove the Hidden) access classes
        obj.rdf_type.add(umdaccess.Published)
        if force_hidden:
            obj.rdf_type.add(umdaccess.Hidden)
        elif force_visible:
            obj.rdf_type.remove(umdaccess.Hidden)

        # save changes
        self.update()

        return handle

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
