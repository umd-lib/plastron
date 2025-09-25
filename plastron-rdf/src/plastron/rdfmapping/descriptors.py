from typing import Any, Callable, Container

from rdflib import URIRef, Literal

from plastron.rdfmapping.embed import EmbeddedObject
from plastron.rdfmapping.properties import RDFDataProperty, RDFObjectProperty, RDFProperty

OBJECT_CLASSES = {}


class Property:
    def __init__(
            self,
            predicate: URIRef,
            required: bool = False,
            repeatable: bool = False,
            values_from: Container = None,
            validate: Callable[[Any], bool] = None,
    ):
        self.predicate = predicate
        self.required = required
        self.repeatable = repeatable
        self.values_from = values_from
        self.validate = validate

    def __set_name__(self, owner, name):
        self.name = name
        self.private_name = f'_{name}'

    def __get__(self, instance, owner):
        if instance is None:
            return self
        if not hasattr(instance, self.private_name):
            setattr(instance, self.private_name, self._get_property(instance))
        return getattr(instance, self.private_name)

    def __set__(self, instance, value):
        prop = self.__get__(instance, instance.__class__)
        prop.clear()
        prop.add(value)

    def _get_property(self, instance) -> RDFProperty:
        return RDFProperty(**self._get_property_kwargs(instance))

    def _get_property_kwargs(self, instance) -> dict[str, Any]:
        return {
            'resource': instance,
            'attr_name': self.name,
            'predicate': self.predicate,
            'required': self.required,
            'repeatable': self.repeatable,
            'values_from': self.values_from,
            'validate': self.validate,
        }


class ObjectProperty(Property):
    def __init__(
            self,
            predicate: URIRef,
            required: bool = False,
            repeatable: bool = False,
            values_from: Container = None,
            validate: Callable[[Any], bool] = None,
            cls: type | str = None,
            embed: bool = False,
    ):
        super().__init__(predicate, required, repeatable, values_from, validate)
        self.object_class = cls
        self.embed = embed

    def _get_property(self, instance) -> RDFObjectProperty:
        if isinstance(self.object_class, str):
            self.object_class = OBJECT_CLASSES[self.object_class]
        return RDFObjectProperty(
            **self._get_property_kwargs(instance),
            object_class=self.object_class,
            embedded=self.embed,
        )

    def __set__(self, instance, value):
        if isinstance(value, URIRef):
            v = value
        elif isinstance(value, EmbeddedObject):
            # here we finally instantiate the embedded object, linking its graph
            # to this instance's graph
            v = value.embed(instance)
        elif hasattr(value, 'uri'):
            v = URIRef(value.uri)
        else:
            # coerce non-URIRef values (e.g., strings) into URIRefs
            v = URIRef(str(value))
        super().__set__(instance, v)


class DataProperty(Property):
    def __init__(
            self,
            predicate: URIRef,
            required: bool = False,
            repeatable: bool = False,
            values_from: Container = None,
            validate: Callable[[Any], bool] = None,
            datatype: URIRef = None,
    ):
        super().__init__(predicate, required, repeatable, values_from, validate)
        self.datatype = datatype

    def _get_property(self, instance) -> RDFDataProperty:
        return RDFDataProperty(
            **self._get_property_kwargs(instance),
            datatype=self.datatype,
        )

    def __set__(self, instance, value):
        # coerce non-Literal values (e.g., strings) into Literals with the datatype
        if not isinstance(value, Literal):
            v = Literal(str(value), datatype=self.datatype)
        else:
            v = value
        super().__set__(instance, v)
