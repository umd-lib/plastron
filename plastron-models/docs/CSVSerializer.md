# CSV Serializer

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

## Multiple Values

Multiple values for the same non-embedded field are separated by a pipe 
character ("|"). Values for different embedded objects of the same field 
(e.g., multiple authors) are separated by a semicolon (";").

These can be mixed, if there are multiple embedded objects and at least 
one has multiple values for the same field.

## Multiple Languages

The deserializer supports two formats for specifying a language tag for 
certain values:

1. [Language-specific columns](#language-specific-columns)
2. [Value-level language tags](#value-level-language-tags)

The serializer, however, only supports writing value-level language tags, 
in order to ensure export-to-import round trip capability.

### Language-specific columns

The language code is appended to the column header like this:

```
Title [de]
```

Only values with that language tag appear in that column. This can lead to 
a variable number of columns for each field, depending on the number of 
distinct language tags for that field's values.

This format is only supported by the `import` command; the `export` 
command always uses the value-level langauge tags.

### Value-level language tags

The language code is prepended to the actual data value like this:

```
[@de]Der Prozeß
```

This format allow mixing of languages in a single column:

```
[@de]Der Prozeß|[@en]The Trial
```

And you can combine tagged and untagged values:

```
The Trial|[@de]Der Prozeß
```

This format is closer to the RDF data model, where the language is an 
attribute of the value, and thus provides a more direct representation of 
the underlying data structure.
