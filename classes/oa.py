from rdflib import Literal
from classes import ldp
from namespaces import dcterms, oa, rdf

# alias the RDFlib Namespace
ns = oa

# Annotation resources

class Annotation(ldp.Resource):
    def __init__(self):
        super(Annotation, self).__init__()
        self.motivation = None

    def add_body(self, body):
        self.linked_objects.append((oa.hasBody, body))
        self.title = body.title
        body.annotation = self

    def add_target(self, target):
        self.linked_objects.append((oa.hasTarget, target))
        target.annotation = self

    def graph(self):
        graph = super(Annotation, self).graph()
        graph.add((self.uri, rdf.type, oa.Annotation))
        if self.motivation is not None:
            graph.add((self.uri, oa.motivatedBy, self.motivation))
        return graph

class TextualBody(ldp.Resource):
    def __init__(self, value, content_type):
        super(TextualBody, self).__init__()
        self.value = value
        self.content_type = content_type
        if len(self.value) <= 25:
            self.title = self.value
        else:
            self.title = self.value[:24] + '…'

    def graph(self):
        graph = super(TextualBody, self).graph()
        graph.add((self.uri, rdf.value, Literal(self.value)))
        graph.add((self.uri, dcterms['format'], Literal(self.content_type)))
        graph.add((self.uri, rdf.type, oa.TextualBody))
        return graph

class SpecificResource(ldp.Resource):
    def __init__(self, source):
        super(SpecificResource, self).__init__()
        self.source = source

    def add_selector(self, selector):
        self.title = selector.title
        self.linked_objects.append((oa.hasSelector, selector))
        selector.annotation = self

    def graph(self):
        graph = super(SpecificResource, self).graph()
        graph.add((self.uri, oa.hasSource, self.source.uri))
        graph.add((self.uri, rdf.type, oa.SpecificResource))
        return graph

class FragmentSelector(ldp.Resource):
    def __init__(self, value, conforms_to=None):
        super(FragmentSelector, self).__init__()
        self.value = value
        self.conforms_to = conforms_to
        self.title = self.value

    def graph(self):
        graph = super(FragmentSelector, self).graph()
        graph.add((self.uri, rdf.value, Literal(self.value)))
        graph.add((self.uri, rdf.type, oa.FragmentSelector))
        if self.conforms_to is not None:
            graph.add((self.uri, dcterms.conformsTo, self.conforms_to))
        return graph

class XPathSelector(ldp.Resource):
    def __init__(self, value):
        super(XPathSelector, self).__init__()
        self.value = value
        self.title = self.value

    def graph(self):
        graph = super(XPathSelector, self).graph()
        graph.add((self.uri, rdf.value, Literal(self.value)))
        graph.add((self.uri, rdf.type, oa.XPathSelector))
        return graph
