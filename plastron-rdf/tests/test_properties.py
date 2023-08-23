from rdflib import Literal

from plastron.rdfmapping.resources import RDFResource


def test_add_remove_values():
    resource = RDFResource(label=Literal('foo'))
    assert len(resource.label) == 1
    resource.label.add(Literal('bar'))
    assert len(resource.label) == 2
    resource.label.clear()
    assert len(resource.label) == 0
