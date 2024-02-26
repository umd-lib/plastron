# Serializers

## CSV Serializer

The mapping from RDF description objects to tabular (e.g., CSV) data is 
defined by a *header map*:

```python
header_map = {
    'title': 'Title',
    'identifier': 'Identifier',
}
```

The header map also supports embedded object properties:

```python
header_map = {
    # title, identifier as above
    'title': 'Title',
    'identifier': 'Identifier',
    # an embedded Person
    'creator': {
        'name': 'Name',
        'homepage': 'Website',
    },
}
```

In this case, the `creator` attribute of the description class **MUST**:

* be an ObjectProperty
* define a `cls` attribute

Since the order of values in a multivalued embedded property matters, the 
serializer will also create an `INDEX` column that gives the ordinal 
position for each embedded (i.e., hash URI) resource. For example:

```
attr[0]=#foo;attr[1]=#bar;other_attr[0]=#flip
```

In this case, `attr` has two values, with the fragment identifiers `#foo` 
and `#bar`. Thus, any column which maps to a property of an object value 
of `attr` will provide the values for the `#foo` object first and the 
`#bar` object second.

So, with this model and header map setup:

```python
from plastron.namespaces import dcterms, foaf
from plastron.rdfmapping.descriptors import DataProperty, ObjectProperty
from plastron.rdfmapping.resources import RDFResource

class Person(RDFResource):
    name = DataProperty(foaf.name)
    homepage = ObjectProperty(foaf.homepage)

class Book(RDFResource):
    title = DataProperty(dcterms.title)
    identifier = DataProperty(dcterms.identifier, repeatable=True)
    creator = ObjectProperty(dcterms.creator, repeatable=True, cls=Person)
    
header_map = {
    'title': 'Title',
    'identifier': 'Identifier',
    'creator': {
        'name': 'Author Name',
        'homepage': 'Author Website',
    },
}
```

And this source RDF:

```turtle
@prefix dcterms: <http://purl.org/dc/terms/> .

<http://example.com/item> dcterms:title "Good Omens" ;
    dcterms:identifier "0060853980", "978-0060853983" ;
    dcterms:creator <http://example.com/item#gaiman>, <http://example.com/item#pratchett> .
    
<http://example.com/item#gaiman> foaf:name "Neil Gaiman" ;
    foaf:homepage <https://neilgaiman.com> .
    
<http://example.com/item#pratchett> foaf:name "Terry Pratchett" ;
    foaf:homepage <https://terrypratchett.com> .
```

We expect this CSV serialization:

```csv
Title,Identifier,Author Name,Author Website,URI,INDEX
Good Omens,0060853980|978-0060853983,Neil Gaiman;Terry Pratchett,https://neilgaiman.com;https://terrypratchett.com,http://example.com/item,creator[0]=#gaiman;creator[1]=#pratchett
```
