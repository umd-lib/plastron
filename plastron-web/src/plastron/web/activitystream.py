import json
import logging
from typing import List
from uuid import uuid4

from flask import Blueprint, Response, current_app, jsonify, request
from rdflib import Graph

from plastron.cli.commands.publish import publish
from plastron.cli.commands.unpublish import unpublish
from plastron.namespaces import activitystreams, rdf, umdact

logger = logging.getLogger(__name__)

activitystream_bp = Blueprint('activitystream', __name__, template_folder='templates')


@activitystream_bp.route('/inbox', methods=['POST'])
def new_activity():
    try:
        activity = Activity(from_json=request.get_json())
        ctx = current_app.config['CONTEXT']
        cmd = get_command(activity)
        cmd(ctx, uris=activity.objects, force_hidden=activity.force_hidden, force_visible=False)
        return Response(status=201)
    except ValidationError as e:
        logger.error(f'Exception: {e}')
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        logger.error(f'Exception: {e}')
        return jsonify({'error': str(e)}), 500


def get_command(activity):
    if activity.publish:
        return publish
    elif activity.unpublish:
        return unpublish
    else:
        raise ValidationError(f'Invalid JSON-LD provided: unsupported activity type.')


class Activity:
    def __init__(self, from_json: str):
        self.id = uuid4()
        self._objects = []
        self._publish = False
        self._unpublish = False
        self._force_hidden = False
        g = Graph()
        g.parse(data=json.dumps(from_json), format='json-ld')

        for s, p, o in g:
            if activitystreams.object == p:
                self._objects.append(str(o))
            elif rdf.type == p:
                if o == umdact.Publish:
                    self._publish = True
                elif o == umdact.Unpublish:
                    self._unpublish = True
                elif o == umdact.PublishHidden:
                    self._publish = True
                    self._force_hidden = True
                else:
                    raise ValidationError(f'Invalid Activity type: {str(o)}')
        if not self.publish and not self.unpublish:
            raise ValidationError(f'Invalid JSON-LD provided: Type not specified.')
        if not self.objects:
            raise ValidationError(f'Invalid JSON-LD provided: Object(s) not specified.')

    def __str__(self):
        return self.id

    @property
    def publish(self) -> bool:
        return self._publish

    @property
    def unpublish(self) -> bool:
        return self._unpublish

    @property
    def force_hidden(self) -> bool:
        return self._force_hidden

    @property
    def objects(self) -> List[str]:
        return self._objects


class ValidationError(Exception):
    pass
