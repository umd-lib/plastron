from rdflib import RDF

from plastron.namespaces import dcterms, oa, prov
from plastron.rdf import ldp, rdf

# alias the rdflib Namespace
ns = oa


# Annotation resources
@rdf.object_property('body', oa.hasBody, embed=True)
@rdf.object_property('target', oa.hasTarget, embed=True)
@rdf.object_property('motivation', oa.motivatedBy)
@rdf.rdf_class(oa.Annotation)
class Annotation(ldp.Resource):
    def __str__(self):
        return ' '.join([str(body) for body in self.body])

    def add_body(self, body):
        self.body.append(body)
        body.annotation = self

    def add_target(self, target):
        self.target.append(target)
        target.annotation = self


@rdf.data_property('value', RDF.value)
@rdf.data_property('content_type', dcterms['format'])
@rdf.rdf_class(oa.TextualBody)
class TextualBody(ldp.Resource):
    def __str__(self):
        value = str(self.value)
        return value if len(value) <= 25 else value[:24] + '…'


@rdf.object_property('selector', oa.hasSelector, embed=True)
@rdf.object_property('source', oa.hasSource)
@rdf.rdf_class(oa.SpecificResource)
class SpecificResource(ldp.Resource):
    def __str__(self):
        return ' '.join([str(selector) for selector in self.selector])

    def add_selector(self, selector):
        self.selector.append(selector)
        selector.annotation = self


@rdf.data_property('value', RDF.value)
@rdf.object_property('conforms_to', dcterms.conformsTo)
@rdf.rdf_class(oa.FragmentSelector)
class FragmentSelector(ldp.Resource):
    def __str__(self):
        return str(self.value)


@rdf.data_property('value', RDF.value)
@rdf.rdf_class(oa.XPathSelector)
class XPathSelector(ldp.Resource):
    def __str__(self):
        return str(self.value)


@rdf.object_property('derived_from', prov.wasDerivedFrom)
class FullTextAnnotation(Annotation):
    pass
