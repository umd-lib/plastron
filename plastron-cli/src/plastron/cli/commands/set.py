import logging
from argparse import Namespace
from collections import defaultdict
from typing import Iterable, Type

from rdflib import Literal

from plastron.cli.commands import BaseCommand
from plastron.models import get_model_from_name
from plastron.rdfmapping.descriptors import DataProperty, ObjectProperty
from plastron.rdfmapping.resources import RDFResourceBase
from plastron.repo import RepositoryResource
from plastron.utils import uri_or_curie

logger = logging.getLogger(__name__)


def configure_cli(subparsers):
    parser = subparsers.add_parser(
        name='set',
        description='Set property values on objects',
    )
    parser.add_argument(
        '-m', '--model',
        dest='model_name',
        required=True,
        help='name of the model class of the objects',
    )
    parser.add_argument(
        '-F', '--field',
        action='append',
        nargs=2,
        dest='fields_to_set',
        metavar=('FIELD_NAME', 'VALUE'),
        help=(
            'field to set; for object properties, VALUE should be either '
            'an absolute http:// or https:// URI, or a CURIE with a known prefix'
        ),
        default=[],
    )
    parser.add_argument(
        'uris',
        nargs='*',
    )
    parser.set_defaults(cmd_name='set')


class Command(BaseCommand):
    def __call__(self, args: Namespace):
        # mimicking a click.Context object to bridge between argparse and click commands
        ctx = Namespace(obj=self.context)
        return set_fields(ctx, args.model_name, args.fields_to_set, args.uris)


def get_new_values(model_class: Type[RDFResourceBase], fields_to_set: Iterable[tuple[str, str]]) -> dict[str, set]:
    values = defaultdict(set)
    for field_name, value in fields_to_set:
        prop = getattr(model_class, field_name)
        if isinstance(prop, DataProperty):
            values[field_name].add(Literal(value, datatype=prop.datatype))
        elif isinstance(prop, ObjectProperty):
            values[field_name].add(uri_or_curie(value))
    return values


def set_fields(ctx, model_name: str, fields_to_set: Iterable[tuple[str, str]], uris: Iterable[str]):
    model_class = get_model_from_name(model_name)
    values = get_new_values(model_class, fields_to_set)
    for uri in uris:
        resource: RepositoryResource = ctx.obj.repo[uri].read()
        obj = resource.describe(model_class)
        for field, new_values in values.items():
            prop = getattr(obj, field)
            prop.update(new_values)
        validation = obj.validate()
        if validation.ok:
            logger.info(f'Resource {uri} is a valid {model_name}')
            resource.update()
            print(obj.uri)
        else:
            for field, error in validation.failures():
                logger.error(f'The value "{error.prop}" for field "{field}" {error}')
            logger.warning(f'Resource {uri} is invalid, skipping')
