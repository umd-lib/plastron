from typing import Type, Dict

from importlib_metadata import entry_points
from rdflib import URIRef

from plastron.rdfmapping.resources import RDFResourceBase, RDFResource

PLUGIN_GROUP = 'plastron.content_models'
CONTENT_MODEL_CLASSES = entry_points(group=PLUGIN_GROUP)


class ModelClassError(Exception):
    pass


class ModelClassNotFoundError(ModelClassError):
    def __init__(self, model_name: str, *args):
        super().__init__(*args)
        self.model_name = model_name


class ContentModeledResource(RDFResourceBase):
    model_name: str
    is_top_level: bool = False
    HEADER_MAP: Dict = None


def get_model_from_name(model_name: str) -> Type[ContentModeledResource]:
    try:
        return CONTENT_MODEL_CLASSES[model_name].load()
    except KeyError as e:
        raise ModelClassNotFoundError(model_name) from e


def get_model_from_uri(rdf_type: URIRef) -> Type[ContentModeledResource]:
    for plugin in CONTENT_MODEL_CLASSES:
        cls = plugin.load()
        if rdf_type in cls.default_values.get('rdf_type', set()):
            return cls
    raise ModelClassNotFoundError(str(rdf_type))


def guess_model(resource: RDFResource) -> Type[ContentModeledResource]:
    for plugin in CONTENT_MODEL_CLASSES:
        cls = plugin.load()
        if cls.default_values.get('rdf_type', set()) <= set(resource.rdf_type.values):
            return cls
    raise ModelClassError()
