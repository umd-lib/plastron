from plastron.models.acl import ACL, Authorization
from rdflib import RDF, Graph, URIRef

from plastron.namespaces import acl, webac


def test_acl_authorizations():
    graph = Graph()
    acl_uri = URIRef('https://example.org/acl')
    authorization_uri = URIRef('https://example.org/authorization')

    graph.add((authorization_uri, RDF.type, acl.Authorization))
    graph.add((authorization_uri, acl.agent, URIRef('https://example.org/user')))
    graph.add((authorization_uri, acl.mode, acl.Read))
    graph.add((authorization_uri, acl.accessTo, URIRef('https://example.org/resource')))

    resource = ACL(uri=acl_uri, graph=graph)
    triples_before = set(resource.graph)

    authorizations = list(resource.authorizations())

    assert len(authorizations) == 1
    assert isinstance(authorizations[0], Authorization)
    assert authorizations[0].uri == authorization_uri

    assert set(authorizations[0].agent.values) == {URIRef('https://example.org/user')}
    assert set(authorizations[0].mode.values) == {acl.Read}

    assert authorizations[0].access_to.value == URIRef('https://example.org/resource')
    assert set(resource.graph) == triples_before
    assert 'authorizations' not in resource.rdf_property_names


def test_acl_and_authorization_rdf_types_and_properties():
    resource = ACL()
    authorization = Authorization(
        agent=[URIRef('https://example.org/user')],
        mode=[acl.Read],
        access_to=URIRef('https://example.org/resource'),
    )

    assert webac.Acl in resource.rdf_type.values
    assert acl.Authorization in authorization.rdf_type.values

    assert authorization.agent.required
    assert authorization.agent.repeatable

    assert authorization.mode.required
    assert authorization.mode.repeatable

    assert authorization.access_to.required
    assert not authorization.access_to.repeatable

    assert not authorization.default.required
    assert not authorization.default.repeatable
