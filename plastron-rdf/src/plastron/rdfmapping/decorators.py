from typing import Callable, Any


def default(name: str, *values, required: bool = False):
    """Decorator that set the default values for the named attribute of the decorated
    class. If `required` is `True`, also add a validator that checks that all the default
    values are present in the named attribute."""
    def decorator(cls):
        cls.default_values[name].update(values)
        if required:
            def validator(obj):
                prop = getattr(obj, name)
                return all(v in prop for v in values)
            cls.validators.append(validator)
        return cls
    return decorator


def rdf_type(*values, **kwargs):
    """Shortcut for `@default('rdf_type', *values, **kwargs)`"""
    return default('rdf_type', *values, **kwargs)


def validate(func: Callable[[Any], bool]):
    """Decorator that adds the given function as a validator to the decorated class. The
    validator function is called by `plastron.rdfmapping.resources.RDFResourceBase.is_valid()`,
    with the instance object as its only argument. Its primary use is for validations that
    require checking more than one attribute, as opposed to the per-attribute validators."""
    def decorator(cls):
        cls.validators.append(func)
        return cls
    return decorator
