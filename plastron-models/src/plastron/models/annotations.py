from rdflib import RDF, URIRef, Literal

from plastron.namespaces import dcterms, oa, prov, sc
from plastron.rdfmapping.decorators import rdf_type
from plastron.rdfmapping.descriptors import ObjectProperty, DataProperty
from plastron.rdfmapping.embed import embedded
from plastron.rdfmapping.resources import RDFResource

# alias the rdflib Namespace
ns = oa


# Annotation resources
@rdf_type(oa.Annotation)
class Annotation(RDFResource):
    body = ObjectProperty(oa.hasBody, repeatable=True, cls=RDFResource)
    target = ObjectProperty(oa.hasTarget, cls=RDFResource)
    motivation = ObjectProperty(oa.motivatedBy)

    def __str__(self):
        return ' '.join([str(body) for body in self.body])

    def add_body(self, body):
        self.body.append(body)
        body.annotation = self

    def add_target(self, target):
        self.target.append(target)
        target.annotation = self


@rdf_type(oa.TextualBody)
class TextualBody(RDFResource):
    value = DataProperty(RDF.value)
    content_type = DataProperty(dcterms['format'])

    def __str__(self):
        value = str(self.value)
        return value if len(value) <= 25 else value[:24] + '…'


@rdf_type(oa.SpecificResource)
class SpecificResource(RDFResource):
    selector = ObjectProperty(oa.hasSelector, cls=RDFResource)
    source = ObjectProperty(oa.hasSource)

    def __str__(self):
        return ' '.join([str(selector) for selector in self.selector])

    def add_selector(self, selector):
        self.selector.append(selector)
        selector.annotation = self


@rdf_type(oa.FragmentSelector)
class FragmentSelector(RDFResource):
    value = DataProperty(RDF.value)
    conforms_to = ObjectProperty(dcterms.conformsTo)

    def __str__(self):
        return str(self.value)


@rdf_type(oa.XPathSelector)
class XPathSelector(RDFResource):
    value = DataProperty(RDF.value)

    def __str__(self):
        return str(self.value)


class FullTextAnnotation(Annotation):
    derived_from = ObjectProperty(prov.wasDerivedFrom)


class TextblockOnPage(Annotation):
    derived_from = ObjectProperty(prov.wasDerivedFrom, cls=RDFResource)

    @classmethod
    def from_textblock(cls, textblock, page, scale, ocr_file):
        xywh = ','.join([str(i) for i in textblock.xywh(scale)])
        return cls(
            body=embedded(TextualBody)(
                value=textblock.text(scale=scale),
                content_type='text/plain'
            ),
            target=embedded(SpecificResource)(
                source=URIRef(page.url),
                selector=embedded(FragmentSelector)(
                    value=Literal(f'xywh={xywh}'),
                    conforms_to=URIRef('http://www.w3.org/TR/media-frags/'),
                ),
            ),
            derived_from=embedded(SpecificResource)(
                source=URIRef(ocr_file.url),
                selector=embedded(XPathSelector)(value=f'//*[@ID="{textblock.id}"]'),
            ),
            motivation=sc.painting
        )
