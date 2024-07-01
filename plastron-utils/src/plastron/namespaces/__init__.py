"""Useful namespaces for use with `rdflib` code."""

import sys
from typing import Optional

from rdflib import Namespace, Graph
from rdflib.namespace import NamespaceManager

acl = Namespace('http://www.w3.org/ns/auth/acl#')
"""[Web Access Controls (WebAC)](https://solidproject.org/TR/wac)"""

activitystreams = Namespace('https://www.w3.org/ns/activitystreams#')
"""[Activity Streams 2.0](https://www.w3.org/TR/activitystreams-core/)"""

bibo = Namespace('http://purl.org/ontology/bibo/')
"""[Bibliographic Ontology](https://www.dublincore.org/specifications/bibo/bibo/)"""

carriers = Namespace('http://id.loc.gov/vocabulary/carriers/')
"""[Library of Congress Carriers Schema](https://id.loc.gov/vocabulary/carriers.html)"""

dc = Namespace('http://purl.org/dc/elements/1.1/')
"""[Dublin Core Elements 1.1](https://www.dublincore.org/specifications/dublin-core/dcmi-terms/#section-3)"""

dcmitype = Namespace('http://purl.org/dc/dcmitype/')
"""[Dublin Core Type Vocabulary](https://www.dublincore.org/specifications/dublin-core/dcmi-terms/#section-7)"""

dcterms = Namespace('http://purl.org/dc/terms/')
"""[Dublin Core Terms](https://www.dublincore.org/specifications/dublin-core/dcmi-terms/#section-2)"""

ebucore = Namespace('http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#')
"""[European Broadcasting Union (EBU) Core](https://www.ebu.ch/metadata/ontologies/ebucore/)"""

edm = Namespace('http://www.europeana.eu/schemas/edm/')
"""[Europeana Data Model](https://pro.europeana.eu/page/edm-documentation)"""

ex = Namespace('http://www.example.org/terms/')
"""Example Namespace"""

fabio = Namespace('http://purl.org/spar/fabio/')
"""[FRBR-aligned Bibliographic Ontology](https://sparontologies.github.io/fabio/current/fabio.html)"""

fedora = Namespace('http://fedora.info/definitions/v4/repository#')
"""[Fedora Commons Repository Ontology](https://fedora.info/definitions/v4/2016/10/18/repository)"""

foaf = Namespace('http://xmlns.com/foaf/0.1/')
"""[FOAF ("Friend-of-a-friend") Vocabulary](http://xmlns.com/foaf/0.1/)"""

geo = Namespace('http://www.w3.org/2003/01/geo/wgs84_pos#')
"""[WGS84 Geo Positioning](https://www.w3.org/2003/01/geo/wgs84_pos)"""

iana = Namespace('http://www.iana.org/assignments/relation/')
"""IANA Link Relations"""

ldp = Namespace('http://www.w3.org/ns/ldp#')
"""[Linked Data Platform](https://www.w3.org/TR/ldp/)"""

ndnp = Namespace('http://chroniclingamerica.loc.gov/terms/')
"""[National Digital Newspaper Program (NDNP) Vocabulary](https://chroniclingamerica.loc.gov/terms/)

**Note:** This namespace is actually incorrect; it should end with "#" and not "/". Unfortunately,
correction would require modification of many resources in fcrepo."""

oa = Namespace('http://www.w3.org/ns/oa#')
"""[Web Annotations](https://www.w3.org/TR/annotation-vocab/)"""

ore = Namespace('http://www.openarchives.org/ore/terms/')
"""[OAI Object Reuse and Exchange (ORE)](http://openarchives.org/ore/1.0/vocabulary)"""

owl = Namespace('http://www.w3.org/2002/07/owl#')
"""[Web Ontology Language (OWL)](https://www.w3.org/TR/owl2-syntax/)"""

pcdm = Namespace('http://pcdm.org/models#')
"""[Portland Common Data Model (PCDM)](https://pcdm.org/2016/04/18/models)"""

pcdmuse = Namespace('http://pcdm.org/use#')
"""[PCDM Use Extension](https://pcdm.org/2021/04/09/use)"""

premis = Namespace('http://www.loc.gov/premis/rdf/v1#')
"""[Preservation Metadata: Implementation Strategies (PREMIS)](https://id.loc.gov/ontologies/premis-1-0-0.html)"""

prov = Namespace('http://www.w3.org/ns/prov#')
"""[Provenance Ontology (PROV-O)](https://www.w3.org/TR/prov-o/)"""

rdf = Namespace('http://www.w3.org/1999/02/22-rdf-syntax-ns#')
"""[RDF](https://www.w3.org/TR/rdf11-schema/)"""

rdfs = Namespace('http://www.w3.org/2000/01/rdf-schema#')
"""[RDF Schema](https://www.w3.org/TR/rdf11-schema/)"""

rel = Namespace('http://id.loc.gov/vocabulary/relators/')
"""[Library of Congress Relator Terms](https://id.loc.gov/vocabulary/relators.html)"""

sc = Namespace('http://www.shared-canvas.org/ns/')
"""[Shared Canvas Data Model](https://iiif.io/api/model/shared-canvas/1.0/)"""

schema = Namespace('https://schema.org/')
"""[Schema.org](https://schema.org/)"""

skos = Namespace('http://www.w3.org/2004/02/skos/core#')
"""[Simple Knowledge Organization System (SKOS)](https://www.w3.org/TR/skos-reference/)"""

umd = Namespace('http://vocab.lib.umd.edu/model#')
"""[UMD Content Models Vocabulary](http://vocab.lib.umd.edu/model)"""

umdaccess = Namespace('http://vocab.lib.umd.edu/access#')
"""[UMD Access Classes Vocabulary](http://vocab.lib.umd.edu/access)"""

umdform = Namespace('http://vocab.lib.umd.edu/form#')
"""[UMD Genre/Form Vocabulary](http://vocab.lib.umd.edu/form)"""

umdtype = Namespace('http://vocab.lib.umd.edu/datatype#')
"""[UMD Datatypes Vocabulary](http://vocab.lib.umd.edu/datatype)"""

umdact = Namespace('http://vocab.lib.umd.edu/activity#')
"""[UMD Activity Types Vocabulary](http://vocab.lib.umd.edu/activity)"""

webac = Namespace('http://fedora.info/definitions/v4/webac#')
"""[Fedora Commons WebAC Ontology](https://fedora.info/definitions/v4/2015/09/03/webac)"""

xsd = Namespace('http://www.w3.org/2001/XMLSchema#')
"""[XML Schema Datatypes](https://www.w3.org/TR/xmlschema-2/#built-in-datatypes)"""


def get_manager(graph: Optional[Graph] = None) -> NamespaceManager:
    """Scan this module's attributes for `Namespace` objects, and bind them
    to a prefix corresponding to their attribute name defined above."""
    if graph is None:
        graph = Graph()
    nsm = NamespaceManager(graph)
    prefixes = {attr: value for attr, value in sys.modules[__name__].__dict__.items() if isinstance(value, Namespace)}
    for prefix, ns in prefixes.items():
        nsm.bind(prefix, ns)
    return nsm


namespace_manager = get_manager()
