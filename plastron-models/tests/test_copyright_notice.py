from plastron.models.umd import Item
from plastron.models.letter import Letter
from plastron.models.newspaper import Issue
from plastron.models.poster import Poster
from plastron.namespaces import schema
from rdflib import Graph, URIRef, Literal

import pytest

base_uri = 'http://example.com/xyz'


@pytest.mark.parametrize("model_class", [Item, Letter, Poster, Issue])
def test_model_with_copyright_notice(model_class):
    copyright_text = 'Copyright 2024. All rights reserved.'
    model = create_model_with_copyright_notice(model_class, base_uri, copyright_text)
    assert str(model.copyright_notice.value) == copyright_text


@pytest.mark.parametrize("model_class", [Item, Letter, Poster, Issue])
def test_copyright_notice_can_be_set_on_model(model_class):
    copyright_notice = 'Public Domain'

    model = model_class(uri=base_uri)
    model.copyright_notice = Literal(copyright_notice)

    expected = (URIRef(base_uri), URIRef('https://schema.org/copyrightNotice'), Literal(copyright_notice))
    assert expected in model.graph


# Helper Functions

def create_model_with_copyright_notice(model_class, item_uri, copyright_notice_text):
    copyright_notice = f'<> <{schema.copyrightNotice}> \"{copyright_notice_text}\" .'
    model_graph = Graph().parse(data=copyright_notice, format='turtle', publicID=item_uri)
    model = model_class(graph=model_graph, uri=item_uri)

    return model
