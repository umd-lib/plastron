import pytest
from rdflib import Literal

from plastron.models.umd import Item


@pytest.fixture
def header_map():
    return {
        'title': 'Title',
        'subject': {
            'label': 'Subject',
            'same_as': 'Subject URI',
        },
    }


@pytest.fixture
def multilingual_item():
    return Item(title=[Literal('The Trial'), Literal('Der Prozeß', lang='de')])
