# plastron-rdf

RDF-to-Python mapping framework

## How it works

An *RDF resource class* encapsulates a subject URI, an RDF graph, a set of 
inserts and deletes made to that graph, and a mapping from particular RDF 
predicates to Python attributes, for easy access and manipulation of the 
RDF graph.

The mapping is created by declaring class attributes on the RDF resource
class using one of two *descriptor classes*: **DataProperty** or
**ObjectProperty**. Use a DataProperty when all the values of its
predicate will be RDF literals. Use an ObjectProperty when the values will
be URIs.

Each descriptor takes one required argument, and several optional ones. 
The required argument `predicate` is the URI of the RDF predicate for this 
mapping. The optional arguments are:

* `required`: boolean, defaults to `False`
* `repeatable`: boolean, defaults to `False`. If it is `False`, generally 
  it means that a resource is not valid if it has more than a single 
  triple using the same predicate. The exception to this is for 
  DataProperty attributes. A resource may have multiple triples using the 
  same predicate and still be considered valid as long as each Literal 
  value has a different language code.
* `validate`: callable returning a boolean, used for additional 
  custom validation beyond the `required` and `repeatable` built-ins.

For DataProperty attributes, you can also specify:

* `datatype`: URI of the data type of this field. This has two functions:

    1. Used in conjunction with the predicate URI to select the triples to 
       return the values of for this attribute. This allows you to reuse 
       the same predicate URI for different attributes. Note that a 
       DataProperty with `datatype=None` (the default) will only return 
       values *without* a datatype.
       Note that because of the way RDF literals are defined, an attribute 
       cannot have a non-`None` datatype *and* a language code.
  
    2. When adding values to this attribute, if they have no datatype, 
       this datatype is added to them before they are added to the 
       resource's graph.

```python
from rdflib import URIRef

from plastron.rdfmapping.descriptors import DataProperty
from plastron.rdfmapping.resources import RDFResourceBase

# an RDF resource class inherits from the RDFResourceBase abstract class,
# or another subclass thereof
class Book(RDFResourceBase):
    title = DataProperty(
        predicate=URIRef('http://purl.org/dc/terms/title'),
        required=True,
    )
    author = DataProperty(
        predicate=URIRef('http://purl.org/dc/elements/1.1/creator'),
        required=True,
        repeatable=True,
    )
    publication_date = DataProperty(
        predicate=URIRef('http://purl.org/dc/terms/published'),
        datatype=URIRef('http://id.loc.gov/datatypes/edtf/EDTF'),
    )
```

When one of these attributes is accessed from an instance, it returns 
either an **RDFDataProperty** or an **RDFObjectProperty** object.

```pycon
>>> book = Book()
>>> book.title
<plastron.rdfmapping.descriptors.RDFDataProperty object at ...>
```

Using that object, you can manipulate and query its values:

```pycon
>>> book.title = 'Good Omens'  # set the title
>>> str(book.title)            # get it back as a string
'Good Omens'
>>> len(book.title)            # get the number of values for this attribute
1
>>> book.title.add(            # add a second value
...     Literal('The Nice and Accurate Prophecies of Agnes Nutter')
... )
>>> len(book.title)            # see the length change
2
>>> book.title.remove(         # remove the second value
...     Literal('The Nice and Accurate Prophecies of Agnes Nutter')
... )
>>> len(book.title)            # and the length changes back again
1
>>> book.title.clear()         # clear all the values
>>> len(book.title)
0
>>> book.title = 'Good Omens'  # back where we started
```

You can check the validity of the resource as a whole, or of each of its
individual properties. Checking the resource as a whole returns a
ValidationResultsDict object of attribute names mapped to ValidationResult 
objects, while checking each property returns a ValidationResult, either
ValidationSuccess (which evaluates to True in a boolean context) or
ValidationFailure (which evaluates to False).

```pycon
>>> book.is_valid       # this is False, because we have not added any authors
False
>>> book.label.is_valid
<plastron.rdfmapping.properties.ValidationSuccess object at ...>
>>> book.author.is_valid
<plastron.rdfmapping.properties.ValidationFailure object at ...>
>>> bool(book.author.is_valid)  # bool-ifies to False
False
>>> str(book.author.is_valid)   # stringifies to a validation message
'is required'
```

The ValidationResultsDict is a subclass of a regular dictionary, with 
added methods to get the items that are successes and the items that are 
failures.

```pycon
>>> results = book.validate()
>>> results.keys()
dict_keys(['title', 'author', 'date'])
>>> results.values()
dict_values([<plastron.rdfmapping.properties.ValidationSuccess object at ...>,
 <plastron.rdfmapping.properties.ValidationFailure object at ...>,
 <plastron.rdfmapping.properties.ValidationSuccess object at ...>])
>>> {name: str(result) for name, result in results if isinstance(result, ValidationFailure)}
{'author': 'is required'}
>>> [name for name, result in results if isinstance(result, ValidationSuccess)]
['title', 'date']
```

