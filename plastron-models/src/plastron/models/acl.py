from rdflib import RDF

from plastron.namespaces import acl, webac
from plastron.rdfmapping.decorators import rdf_type
from plastron.rdfmapping.descriptors import ObjectProperty
from plastron.rdfmapping.resources import RDFResource


@rdf_type(webac.Acl)
class ACL(RDFResource):
    def authorizations(self):
        for subject in self.graph.subjects(RDF.type, acl.Authorization):
            yield Authorization(uri=subject, graph=self.graph)


@rdf_type(acl.Authorization)
class Authorization(RDFResource):
    agent = ObjectProperty(acl.agent, required=True, repeatable=True)
    mode = ObjectProperty(acl.mode, required=True, repeatable=True)
    access_to = ObjectProperty(acl.accessTo, required=True)
    default = ObjectProperty(acl.default)
