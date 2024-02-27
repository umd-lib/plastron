import logging

from plastron.handles import HandleBearingResource, Handle, HandleServiceClient
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
        if obj.handle.is_valid:
            logger.debug(f'Handle in the repository: {obj.handle.value}')

        handle = handle_client.get_handle(repo_uri=self.url)
        if handle is not None:
            logger.debug(f'Handle service returns handle: {handle.hdl_uri}')

        if handle is None:
            # create a new handle
            logger.debug(f'Minting new handle for {self.url}')
            handle = handle_client.create_handle(
                repo_uri=self.url,
                url=public_url,
            )
            if handle is None:
                # if the handle is still not created, something *really* went wrong
                logger.error(f'Unable to find or create handle for {self.url}')
                raise RepositoryError(f'Unable to publish {self}')

            obj.handle = handle.hdl_uri
        else:
            # check target URL, and update if needed
            if handle.url != public_url:
                logger.warning(f'Current target URL ({handle.url}) does not match the expected URL ({public_url})')
                handle = handle_client.update_handle(handle, url=public_url)

            # check to ensure that the handle matches
            if obj.handle.is_valid:
                if handle.hdl_uri != str(obj.handle.value):
                    logger.warning('Handle values differ; updating the repository to match the handle service')
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
