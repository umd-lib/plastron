from typing import Set, Tuple, Callable, Any, Iterable, TypeVar

from rdflib import Literal, URIRef
from rdflib.term import Identifier, BNode

from plastron.rdfmapping.validation import ValidationResult, ValidationFailure, ValidationSuccess

ObjectType = TypeVar('ObjectType')


class RDFProperty:
    def __init__(
            self,
            resource,
            attr_name: str,
            predicate: URIRef,
            required: bool = False,
            repeatable: bool = False,
            validate: Callable[[Any], bool] = None,
    ):
        self.resource = resource
        self.attr_name = attr_name
        self.predicate = predicate
        self.required = required
        self.repeatable = repeatable
        self._validate = validate

    @property
    def uri(self) -> URIRef:
        """URI of the predicate"""
        return self.predicate

    @property
    def values(self) -> Iterable:
        """Values of this property"""
        return self.resource.graph.with_changes().objects(self.resource.uri, self.predicate)

    @property
    def value(self):
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

    def update(self, new_values: Iterable) -> Tuple[Set, Set]:
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
        for value in values:
            self.add(value)

    @property
    def is_valid(self) -> ValidationResult:
        if self.required and len(self) == 0:
            return ValidationFailure(self, 'is required')
        if not self.repeatable and len(self) > 1:
            return ValidationFailure(self, 'is not repeatable')
        if self._validate is not None:
            if all(self._validate(v) for v in self.values):
                return ValidationSuccess(self)
            else:
                return ValidationFailure(self, f'is not {self._validate.__doc__}')
        return ValidationSuccess(self)


class RDFDataProperty(RDFProperty):
    def __init__(
            self,
            resource,
            attr_name: str,
            predicate: URIRef,
            required: bool = False,
            repeatable: bool = False,
            validate: Callable[[Any], bool] = None,
            datatype: URIRef = None,
    ):
        super().__init__(resource, attr_name, predicate, required, repeatable, validate)
        self.datatype = datatype

    @property
    def values(self):
        return filter(lambda v: v.datatype == self.datatype, super().values)

    @property
    def languages(self):
        return iter(v.language for v in self.values)

    @property
    def is_valid(self) -> ValidationResult:
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
        return ValidationSuccess(self)


class RDFObjectProperty(RDFProperty):
    def __init__(
            self,
            resource,
            attr_name: str,
            predicate: URIRef,
            required: bool = False,
            repeatable: bool = False,
            validate: Callable[[Any], bool] = None,
            object_class: ObjectType = None,
            embedded: bool = False,
    ):
        super().__init__(resource, attr_name, predicate, required, repeatable, validate)
        self.object_class = object_class
        self.embedded = embedded
        self._object_map = {}

    @property
    def objects(self) -> Iterable[ObjectType]:
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
    def object(self) -> ObjectType:
        try:
            return next(iter(self.objects))
        except StopIteration:
            return None

    def add(self, value):
        if hasattr(value, 'uri'):
            obj = URIRef(value.uri)
            if self.object_class is not None:
                self._object_map[obj] = value
        else:
            obj = value
        super().add(obj)

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
        is_valid_result = super().is_valid
        if not is_valid_result:
            return is_valid_result
        # all values must be URIRefs
        if not all(isinstance(v, URIRef) or isinstance(v, BNode) for v in self.values):
            return ValidationFailure(self, 'all values must be URIs or BNodes')
        return ValidationSuccess(self)


class RDFPropertyError(Exception):
    pass
