from datetime import datetime, timezone

from plastron.namespaces import fedora, premis, xsd
from plastron.rdfmapping.descriptors import ObjectProperty, DataProperty
from plastron.rdfmapping.resources import RDFResource
from urlobject import URLObject


class FedoraResource(RDFResource):
    created = DataProperty(fedora.created, datatype=xsd.dateTime)
    created_by = DataProperty(fedora.createdBy)
    last_modified = DataProperty(fedora.lastModified, datatype=xsd.dateTime)
    last_modified_by = DataProperty(fedora.lastModifiedBy)
    parent = ObjectProperty(fedora.hasParent)


class FedoraBinary(FedoraResource):
    fixity_service = ObjectProperty(fedora.hasFixityService)
    digest = ObjectProperty(premis.hasMessageDigest)
    size = DataProperty(premis.hasSize, datatype=xsd.long)


class FixityDetails(RDFResource):
    """Details of a single fixity check."""

    outcome = DataProperty(premis.hasEventOutcome)
    digest = ObjectProperty(premis.hasMessageDigest)
    digest_algorithm = DataProperty(premis.hasMessageDigestAlgorithm)
    size = DataProperty(premis.hasSize, datatype=xsd.long)

    @property
    def timestamp(self) -> datetime:
        """The timestamp of the fixity check is contained in the fragment
        identifier portion of this details object's URI, as a millisecond
        precision UNIX time. This property returns this timestamp as a
        `datetime` object."""
        return datetime.fromtimestamp(
            int(URLObject(self.uri).fragment.removeprefix('fixity/')) / 1000,
            tz=timezone.utc,
        )

    @property
    def is_success(self) -> bool:
        """Whether the fixity check was successful."""
        return str(self.outcome.value) == 'SUCCESS'


class FixityCheck(RDFResource):
    """Model of the resource returned from a Fedora "/fcr:fixity" endpoint
    that contains an embedded object with the details of the fixity check."""

    fixity_details = ObjectProperty(premis.hasFixity, cls=FixityDetails, embed=True)
