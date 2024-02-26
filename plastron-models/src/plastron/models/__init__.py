import sys
from typing import Type

from plastron.models.letter import Letter
from plastron.models.newspaper import Issue
from plastron.models.poster import Poster
from plastron.models.umd import Item
from plastron.rdfmapping.resources import RDFResourceBase


class ModelClassNotFoundError(Exception):
    def __init__(self, model_name: str, *args):
        super().__init__(*args)
        self.model_name = model_name


def get_model_class(model_name: str) -> Type[RDFResourceBase]:
    try:
        return getattr(sys.modules[__package__], model_name)
    except AttributeError as e:
        raise ModelClassNotFoundError(model_name) from e
