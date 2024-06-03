from plastron.models.letter import Letter

base_uri = 'http://example.com/xyz'


def test_letter_invalid_with_no_fields():
    letter = Letter()
    assert not letter.is_valid


def test_letter_valid_with_only_required_fields():
    letter = Letter()

    # Only provide required fields
    letter.identifier = 'test_letter'
    letter.object_type = 'http://purl.org/dc/dcmitype/Text'
    letter.rights = 'http://vocab.lib.umd.edu/rightsStatement#InC-EDU'
    letter.title = 'Test Letter'
    letter.description = 'Test Letter Description'
    letter.language = 'en'
    letter.part_of = 'http://fedora.info/definitions/v4/repository#inaccessibleResource'
    letter.bibliographic_citation = 'Test Bibliographic Citation'
    letter.rights_holder = 'Test Rights Holder'
    letter.type = 'http://purl.org/dc/dcmitype/Text'
    letter.extent = '1 page'

    assert letter.is_valid
