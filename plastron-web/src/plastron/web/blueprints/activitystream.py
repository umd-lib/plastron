import json
import logging
from uuid import uuid4

from flask import Blueprint, current_app, jsonify, request
from rdflib import Graph

from plastron.namespaces import activitystreams, rdf, umdact
from plastron.repo.publish import PublishableResource

logger = logging.getLogger(__name__)

blueprint = Blueprint('activitystream', __name__, template_folder='templates')


@blueprint.route('/inbox', methods=['POST'])
def new_activity():
    try:
        activity = Activity(from_json=request.get_json())
        ctx = current_app.config['CONTEXT']
        for uri in activity.objects:
            resource: PublishableResource = ctx.repo[uri:PublishableResource].read()
            if activity.publish:
                resource.publish(
                    handle_client=ctx.handle_client,
                    public_url=ctx.get_public_url(resource),
                    force_hidden=activity.force_hidden,
                    force_visible=False,
                )
            elif activity.unpublish:
                resource.unpublish(
                    force_hidden=activity.force_hidden,
                    force_visible=False,
                )
        return {}, 201
    except ValidationError as e:
        logger.error(f'Exception: {e}')
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        logger.error(f'Exception: {e}')
        return jsonify({'error': str(e)}), 500


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
    def objects(self) -> list[str]:
        return self._objects


class ValidationError(Exception):
    pass
