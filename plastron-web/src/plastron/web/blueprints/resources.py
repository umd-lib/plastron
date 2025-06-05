import logging

from flask import Blueprint, request, current_app
from pyparsing import ParseException
from rdflib.plugins.sparql import prepareUpdate
from werkzeug.exceptions import BadRequest, UnsupportedMediaType, NotFound, InternalServerError

from plastron.web.flask_problem import ProblemDetailError
from plastron.models import get_model_from_name, ModelClassNotFoundError
from plastron.repo import RepositoryResource, RepositoryError

logger = logging.getLogger(__name__)
blueprint = Blueprint('resource', __name__)


@blueprint.route('/<path:resource_path>', methods=['PATCH'])
def update(resource_path):
    try:
        resource: RepositoryResource = current_app.config['CONTEXT'].repo.get_resource(resource_path).read()
    except RepositoryError as e:
        logger.error(str(e))
        raise NotFound from e

    if request.content_type != 'application/sparql-update':
        raise UnsupportedMediaType

    logger.info(f'Received request to update {resource.url}')

    sparql_text = request.data.decode()
    logger.debug(f'SPARQL Update Query: {sparql_text}')
    try:
        sparql_update = prepareUpdate(sparql_text, base=resource.url)
    except ParseException as e:
        raise SPARQLUpdateProblem(description=f'SPARQL Update Query parsing error: {e}')

    # Apply the update in-memory to the resource graph
    resource.graph.update(sparql_update)

    model_name = request.args.get('model', None)
    if model_name is not None:
        # validate according to the given content model
        try:
            model_class = get_model_from_name(model_name)
        except ModelClassNotFoundError as e:
            raise UnknownContentModel(model_name=e.model_name)

        # Validate the updated in-memory Graph using the model
        logger.info(f'Validating updated object using model "{model_name}"')
        obj = resource.describe(model_class)
        validation = obj.validate()
        if validation.ok:
            logger.info(f'Validation succeeded using model "{model_name}"')
        else:
            errors = {k: str(v) for k, v in validation.failures()}
            logger.error(f'Validation failed: {errors}')
            raise ContentModelValidationFailure(
                error_count=len(errors),
                validation_errors=errors,
                model_name=model_name,
                resource=resource.url,
            )

    try:
        resource.update()
        logger.info(f'Updated resource {resource.url}')
    except RepositoryError as e:
        logger.error(str(e))
        raise InternalServerError('Repository error')

    return '', 204


class SPARQLUpdateProblem(ProblemDetailError, BadRequest):
    name = 'SPARQL Update problem'


class UnknownContentModel(ProblemDetailError, BadRequest):
    name = 'Unrecognized content-model'
    description = '"{model_name}" is not a recognized content-model name'


class ContentModelValidationFailure(ProblemDetailError, BadRequest):
    name = 'Content-model validation failed'
    description = '{error_count} validation error(s) prevented update of {resource} with content-model {model_name}'
