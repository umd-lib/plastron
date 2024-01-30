import json
import logging
from flask import Blueprint, request, jsonify, Response
from rdflib import Graph, Namespace
from typing import List
from uuid import uuid4

from plastron.cli.commands.publish import Command as PublishCommand
from plastron.cli.commands.unpublish import Command as UnpublishCommand
from plastron.namespaces import get_manager, rdf, umdact

logger = logging.getLogger(__name__)

activitystream = Blueprint('activities', __name__, template_folder='templates')

activitystreams = Namespace('https://www.w3.org/ns/activitystreams#')

@activitystream.route('/inbox', methods=['POST'])
def new_activity():
    try:
        activity = Activity(from_json=request.get_json())
        args = {
            'uris': activity.objects,
            'force_hidden': activity.force_hidden,
            'force_visible': False
        }
        if activity.publish:
            PublishCommand(args)
        elif activity.unpublish:
            UnpublishCommand(args)
        return Response(status=201)
    except Exception as e:
        logger.error(f'Exception: {e}')
        return jsonify({'error': str(e)}), 400

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
                self._objects.append(o)
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
        if (not self.publish and not self.unpublish):
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
