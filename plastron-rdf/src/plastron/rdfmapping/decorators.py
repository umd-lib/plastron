from typing import Callable, Any


def default(name: str, *values, required: bool = False):
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
    return default('rdf_type', *values, **kwargs)


def validate(func: Callable[[Any], bool]):
    def decorator(cls):
        cls.validators.append(func)
        return cls
    return decorator
