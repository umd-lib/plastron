from typing import Callable, Any, Iterable, TypeVar, Type, Iterator, Container

from rdflib import Literal, URIRef
from rdflib.term import Identifier, BNode

from plastron.rdfmapping.embed import EmbeddedObject
from plastron.rdfmapping.validation import ValidationResult, ValidationFailure, ValidationSuccess

T = TypeVar('T')


class RDFProperty:
    """An RDF property"""
    def __init__(
            self,
            resource,
            attr_name: str,
            predicate: URIRef,
            required: bool = False,
            repeatable: bool = False,
            values_from: Container = None,
            validate: Callable[[Any], bool] = None,
    ):
        self.resource = resource
        self.attr_name = attr_name
        self.predicate = predicate
        self.required = required
        self.repeatable = repeatable
        self.values_from = values_from
        self._validate = validate

    @property
    def uri(self) -> URIRef:
        """URI of the predicate"""
        return self.predicate

    @property
    def values(self) -> Iterable:
        """Values of this property"""
        return self.resource.graph.objects(self.resource.uri, self.predicate)

    @property
    def value(self):
        """The first value of this property, or `None` if `values` is empty."""
        try:
            return next(iter(self.values))
        except StopIteration:
            return None

    def __iter__(self):
        return self.values

    def __str__(self):
        return ' '.join(iter(self))

    def __len__(self):
        return len(list(self.values))

    def clear(self):
        """Remove all values from this property."""
        for value in iter(self):
            self.remove(value)

    def add(self, value):
        """Add a single value to this property."""
        self.resource.graph.add((self.resource.uri, self.predicate, value))

    def remove(self, value):
        """Remove a single value from this property."""
        self.resource.graph.remove((self.resource.uri, self.predicate, value))

    def update(self, new_values: Iterable) -> tuple[set, set]:
        """Update this property to have only the new values.

        This method takes the set differences between the current values and the
        new values to construct sets of deleted and inserted values, then removes
        and adds those values, respectively.
        """
        old_values_set = set(self.values)
        new_values_set = set(new_values)
        deleted_values = old_values_set - new_values_set
        inserted_values = new_values_set - old_values_set
        for value in deleted_values:
            self.remove(value)
        for value in inserted_values:
            self.add(value)
        # return the sets so the caller could construct a SPARQL update
        return deleted_values, inserted_values

    def extend(self, values: Iterable):
        """Add each value in the `values` iterable to this property"""
        for value in values:
            self.add(value)

    @property
    def is_valid(self) -> ValidationResult:
        """Checks the validity of this property.

        * If the property is required, there must be at least one value
        * If the property is not repeatable, there must be no more than one value
        * If the property has a `values_from` parameter, then all values must be
          contained by that object (i.e., `value in prop.values_from` must be true)
        * All additional validation functions, such as that given in the `validate`
          parameter of the constructor, must return true values

        If all of these conditions are met, returns a
        `plastron.rdfmapping.validation.ValidationSuccess` object.
        Otherwise, returns a
        `plastron.rdfmapping.validation.ValidationFailure` object.
        """
        if self.required and len(self) == 0:
            return ValidationFailure(self, 'is required')
        if not self.repeatable and len(self) > 1:
            return ValidationFailure(self, 'is not repeatable')
        if self.values_from is not None and any(v not in self.values_from for v in self.values):
            return ValidationFailure(self, f'is not from {self.values_from}')
        if self._validate is not None:
            if all(self._validate(v) for v in self.values):
                return ValidationSuccess(self)
            else:
                return ValidationFailure(self, f'is not {self._validate.__doc__}')
        return ValidationSuccess(self)


class RDFDataProperty(RDFProperty):
    """An RDF property whose values are always RDF literals"""
    def __init__(
            self,
            resource,
            attr_name: str,
            predicate: URIRef,
            required: bool = False,
            repeatable: bool = False,
            values_from: Container = None,
            validate: Callable[[Any], bool] = None,
            datatype: URIRef = None,
    ):
        super().__init__(resource, attr_name, predicate, required, repeatable, values_from, validate)
        self.datatype: URIRef = datatype
        """The datatype of this property"""

    def add(self, value):
        if not isinstance(value, Literal):
            raise TypeError(f'Cannot add a non-Literal value {value} to data property {self.attr_name}')
        super().add(value)

    @property
    def values(self) -> Iterator[Literal]:
        return filter(lambda v: isinstance(v, Literal) and v.datatype == self.datatype, super().values)

    @property
    def languages(self) -> Iterator[str]:
        """Returns an iterator over the language tags of this property's values."""
        return iter(v.language for v in self.values)

    @property
    def is_valid(self) -> ValidationResult:
        """Checks the validity of this property.

        Performs all the validity checks from `RDFProperty.is_valid()`, plus
        requires that all values are RDF literals.

        Even if this property is not marked as repeatable, more than one value
        is allowed so long as each value has a different language tag.
        """
        is_valid_result = super().is_valid
        if not is_valid_result:
            # exception to the superclass rule: if repeatable is False but the only difference
            # in the values is their language, it should be valid
            if not self.repeatable and len(self) > 1:
                if len(set(self.languages)) != len(self):
                    return ValidationFailure(self, 'is not repeatable')
            else:
                return is_valid_result
        # all values must be literals
        if not all(isinstance(v, Literal) for v in self.values):
            return ValidationFailure(self, 'all values must be Literals')
        # if required, all values must be non-blank
        if self.required and any(v.strip() == '' for v in self.values):
            return ValidationFailure(self, 'all values must be non-blank')
        return ValidationSuccess(self)


class RDFObjectProperty(RDFProperty):
    """An RDF property whose values are always URIRefs or RDF blank nodes"""
    def __init__(
            self,
            resource,
            attr_name: str,
            predicate: URIRef,
            required: bool = False,
            repeatable: bool = False,
            values_from: Container = None,
            validate: Callable[[Any], bool] = None,
            object_class: Type[T] = None,
            embedded: bool = False,
    ):
        super().__init__(resource, attr_name, predicate, required, repeatable, values_from, validate)
        self.object_class = object_class
        self.embedded = embedded
        self._object_map = {}

    @property
    def objects(self) -> Iterable[T]:
        """Values of this property, represented as full objects. Requires that this property
        has its `object_class` set; otherwise, it raises an `RDFPropertyError`."""
        if self.object_class is None:
            raise RDFPropertyError(f'No object class defined for the property with predicate {self.predicate}')
        for value in self.values:
            if isinstance(value, Identifier):
                if value not in self._object_map:
                    self._object_map[value] = self.object_class(uri=value, graph=self.resource.graph)
                yield self._object_map[value]
            else:
                yield value

    @property
    def object(self) -> T:
        """The first value of this property, represented as an object, or `None` if `values`
        is empty."""
        try:
            return next(iter(self.objects))
        except StopIteration:
            return None

    def add(self, value):
        if hasattr(value, 'uri'):
            uri = URIRef(value.uri)
            if self.object_class is not None:
                self._object_map[uri] = value
        elif isinstance(value, EmbeddedObject):
            obj = value.embed(self.resource)
            uri = obj.uri
            self._object_map[uri] = obj
        else:
            uri = value
        super().add(uri)

    def remove(self, value):
        if hasattr(value, 'uri'):
            obj = URIRef(value.uri)
            if obj in self._object_map:
                del self._object_map[obj]
        else:
            obj = value
        super().remove(obj)

    @property
    def is_valid(self) -> ValidationResult:
        """Checks the validity of this property.

        Performs all the validity checks from `RDFProperty.is_valid()`, plus
        requires that all values are either URIRefs or RDF blank nodes.
        """
        is_valid_result = super().is_valid
        if not is_valid_result:
            return is_valid_result
        # all values must be URIRefs
        if not all(isinstance(v, URIRef) or isinstance(v, BNode) for v in self.values):
            return ValidationFailure(self, 'all values must be URIs or BNodes')
        return ValidationSuccess(self)


class RDFPropertyError(Exception):
    pass
