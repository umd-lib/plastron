from copy import deepcopy, copy

from rdflib import Literal

from plastron.rdfmapping.resources import RDFResource


def test_add_remove_values():
    resource = RDFResource(label=Literal('foo'))
    assert len(resource.label) == 1
    resource.label.add(Literal('bar'))
    assert len(resource.label) == 2
    resource.label.clear()
    assert len(resource.label) == 0


def test_same_obj_same_prop():
    resource = RDFResource()
    # multiple accesses of the property for the same resource object
    # returns the same property object; that is, as long as `a is b`
    # holds, so should `a.prop is b.prop`
    assert resource.label is resource.label
    other = resource
    assert other.label is resource.label
    # shallow copies also retain the same property objects; this is
    # due to the semantics of the `copy` operation, and is unlikely
    # to be very useful in production code; where a copy of a resource
    # is needed, `deepcopy` should be used instead
    shallow_copy = copy(resource)
    assert shallow_copy.label is resource.label


def test_different_obj_different_prop():
    a = RDFResource()
    b = RDFResource()
    assert a.label is not b.label
    c = deepcopy(a)
    assert a.label is not c.label


def test_property_preserves_value():
    resource = RDFResource()
    value = Literal('foo')
    resource.label = value
    assert resource.label.value is value
