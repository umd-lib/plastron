from typing import Set, Tuple, Callable, Any, Iterable

from rdflib import Literal, URIRef
from rdflib.term import Identifier


class RDFProperty:
    def __init__(
            self,
            resource,
            predicate: URIRef,
            required: bool = False,
            repeatable: bool = False,
            validate: Callable[[Any], bool] = None,
    ):
        self.resource = resource
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
    def is_valid(self) -> bool:
        if self.required and len(self) == 0:
            return False
        if not self.repeatable and len(self) > 1:
            return False
        if self._validate is not None:
            return all(self._validate(v) for v in self.values)
        return True


class RDFDataProperty(RDFProperty):
    def __init__(
            self,
            resource,
            predicate: URIRef,
            required: bool = False,
            repeatable: bool = False,
            validate: Callable[[Any], bool] = None,
            datatype: URIRef = None,
    ):
        super().__init__(resource, predicate, required, repeatable, validate)
        self.datatype = datatype

    @property
    def values(self):
        return filter(lambda v: v.datatype == self.datatype, super().values)

    @property
    def languages(self):
        return iter(v.language for v in self.values)

    @property
    def is_valid(self) -> bool:
        if not super().is_valid:
            # exception to the superclass rule: if repeatable is False but the only difference
            # in the values is their language, it should be valid
            if not self.repeatable and len(self) > 1:
                if len(set(self.languages)) != len(self):
                    return False
            else:
                return False
        # all values must be literals
        if not all(isinstance(v, Literal) for v in self.values):
            return False
        return True


class RDFObjectProperty(RDFProperty):
    def __init__(
            self,
            resource,
            predicate: URIRef,
            required: bool = False,
            repeatable: bool = False,
            validate: Callable[[Any], bool] = None,
            object_class: type = None,
            embedded: bool = False,
    ):
        super().__init__(resource, predicate, required, repeatable, validate)
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
                    self._object_map[value] = self.object_class(value)
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
    def is_valid(self) -> bool:
        if not super().is_valid:
            return False
        # all values must be URIRefs
        if not all(isinstance(v, URIRef) for v in self.values):
            return False
        return True


class RDFPropertyError(Exception):
    pass
