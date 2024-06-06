# plastron-models

Metadata content models based on RDF

## Model Packages

* [annotations](src/plastron/models/annotations.py): Auxiliary model
  classes for Web Annotations
* [letter](src/plastron/models/letter.py): Legacy content model for the
  Katherine Anne Porter correspondence collection
* [newspaper](src/plastron/models/newspaper.py): Content model for the
  Student Newspapers collection, based on the NDNP data format
* [poster](src/plastron/models/poster.py): Legacy content model for the
  Prange Posters and Wall Newspapers collection
* [umd](src/plastron/models/umd.py): Standardized digital object content
  model for current and future collections

## Vocabulary Retrieval

The `get_vocabulary` method in the
`plastron-models/src/plastron/validation/vocabularies/__init__.py` module
initializer controls how vocabularies used for validation are retrieved.

Vocabularies used to validate models are retrieved either from the local
filesystem, or from a vocabulary server on the network.

The code uses two variables:

* `VOCABULARIES_DIR` - The full filepath to the directory containing the local
  vocabulary files
* `VOCABULARIES` - A dictionary mapping a URI to the name of the file containing
  the vocabulary.

Vocabularies matching URIs in the `VOCABULARIES` dictionary are first looked
up locally, with the local file being used, if found. If not, a network lookup
using the URI as the vocabulary location is used.

Vocabulary URIs not in the `VOCABULARIES` dictionary are always looked up via
the network.

### Vocabulary Retrieval for Tests

In general, unit tests should be run without making calls to the network, as
making a network call makes the tests slower and less reliable.

The retrieval of the vocabularies via the `__init__.py` module initializer is
problematic for the tests, because the module initialization occurs before a
test is even run. This makes normal methods of overriding the network calls
ineffective. For example, trying to intercept the network calls using the
“httpretty” library doesn’t work, because by the time the
“@httpretty.activate” decorator is accessed, the module has already been
initialized. The same is true when attempting to “monkey patch” the module.

One method that was found to work was to add a
`conftest.py` file into the root directory of the project, with a
`pytest_configure` method. It is necessary to have the `conftest.py` in the
root directory, so that it will always be used when running pytests in any of
the Plastron modules (as those tests may use one of the content models with a
vocabulary). The `pytest_configure` method runs as soon as pytest starts, and
before any modules are loaded, providing an opportunity to set the
“VOCABULARIES_DIR” and “VOCABULARIES” variables to values that are suitable for
testing.

Any vocabularies needed for the tests should be added as follows:

1) Add a file containing the vocabulary (in "turtle" format) to the
  "plastron-models/tests/data/vocabularies/" directory.

2) In the `conftest.py` file in the root directory, add the vocabulary URI and
   filename to the `VOCABULARIES` dictionary.

Note that if a vocabulary is not added, a network call will still be attempted,
due to the fallback behavior of the `get_vocabularies` method.
