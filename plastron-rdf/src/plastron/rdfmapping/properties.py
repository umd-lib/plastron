from typing import Set, Tuple, Callable, Any, Iterable, Optional

from rdflib import Literal, URIRef
from rdflib.term import Identifier, BNode


class ValidationResult:
    def __init__(self, prop: 'RDFProperty', message: Optional[str] = ''):
        self.prop = prop
        self.message = message

    def __str__(self):
        return self.message

    def __bool__(self):
        raise NotImplementedError


class ValidationFailure(ValidationResult):
    def __bool__(self):
        return False


class ValidationSuccess(ValidationResult):
    def __bool__(self):
        return True


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
    def uri(self):
        return self.predicate

    @property
    def values(self):
        return self.resource.graph.objects(self.resource.uri, self.predicate)

    def __iter__(self):
        return self.values

    def __str__(self):
        return ' '.join(iter(self))

    def __len__(self):
        return len(list(self.values))

    def clear(self):
        for value in iter(self):
            self.remove(value)

    def add(self, value):
        self.resource.inserts.add((self.resource.uri, self.predicate, value))

    def remove(self, value):
        self.resource.deletes.add((self.resource.uri, self.predicate, value))

    def update(self, new_values) -> Tuple[Set, Set]:
        # take the set differences to find deleted and inserted values
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
            object_class: type = None,
            embedded: bool = False,
    ):
        super().__init__(resource, attr_name, predicate, required, repeatable, validate)
        self.object_class = object_class
        self.embedded = embedded
        self._object_map = {}

    @property
    def objects(self):
        if self.object_class is None:
            raise RDFPropertyError(f'No object class defined for the property with predicate {self.predicate}')
        for value in self.values:
            if isinstance(value, Identifier):
                if value not in self._object_map:
                    self._object_map[value] = self.object_class(uri=value, graph=self.resource.base_graph)
                yield self._object_map[value]
            else:
                yield value

    def add(self, value):
        if self.object_class is not None and hasattr(value, 'uri'):
            obj = value.uri
            self._object_map[obj] = value
        else:
            obj = value
        super().add(obj)

    def remove(self, value):
        if self.object_class is not None and hasattr(value, 'uri'):
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
