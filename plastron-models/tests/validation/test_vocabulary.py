import pytest

from plastron.validation.vocabularies import Vocabulary


@pytest.fixture
def vocab():
    return Vocabulary('http://purl.org/dc/dcmitype/')


def test_vocabulary(vocab):
    assert str(vocab) == 'http://purl.org/dc/dcmitype/'
    assert len(vocab) == 12
    terms = list(vocab)
    assert len(terms) == 12
    assert len(vocab.items()) == 12


def test_get_item(vocab):
    assert vocab['Image']


def test_get_item_not_exist(vocab):
    with pytest.raises(KeyError):
        _ = vocab['FAKE_TERM']
