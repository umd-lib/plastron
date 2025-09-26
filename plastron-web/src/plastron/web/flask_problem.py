import json
from typing import Any

from werkzeug import Response
from werkzeug.exceptions import HTTPException


class ProblemDetailError(HTTPException):
    """Subclass of the Werkzeug `HTTPException` class that adds a `params`
    dictionary that `as_problem_detail()` uses to format the response details.
    The items in the `params` dictionary are also included as extension
    members in the JSON problem detail response.

    Individual applications should create their own application-specific
    subclasses of this class for exceptions that should result in a problem
    detail response. Those exceptions should also subclass one of the standard
    Werkzeug HTTP exceptions in the `werkzeug.exceptions` module, such as
    `NotFound` or `BadRequest`, in order to provide the correct HTTP response
    codes."""

    name: str
    """Used as the problem detail `title`."""
    description: str
    """Used as the problem detail `details`. The value is treated as a format
    string, and is filled in using the `params` dictionary."""

    def __init__(self, description=None, response=None, **params):
        super().__init__(description, response)
        self.params = params

    def as_problem_detail(self) -> dict[str, Any]:
        """Format the exception information as a dictionary with keys as
        specified in the [RFC 9457](https://www.rfc-editor.org/rfc/rfc9457)
        JSON Problem Details format.

        | RFC 9457 Key | Attribute |
        |--------------|-----------|
        | `"status"`   | `code`    |
        | `"title"`    | `name`    |
        | `"details"`  | `description`, formatted using the `params` dictionary |

        The items in the `params` dictionary are also included in the problem
        details as [extension members](https://www.rfc-editor.org/rfc/rfc9457#name-extension-members).

        """
        return {
            'status': self.code,
            'title': self.name,
            'details': self.description.format(**self.params),
            **self.params,
        }


def problem_detail_response(e: HTTPException) -> Response:
    """Return a JSON Problem Detail ([RFC 9457](https://www.rfc-editor.org/rfc/rfc9457))
    for HTTP errors.

    This function is mainly intended to be registered as an error handler
    with a Flask app:

    ```python
    from flask import Flask
    from flask_problem import ProblemDetailError, problem_detail_response

    app = Flask(__name__)

    ...

    app.register_error_handler(ProblemDetailError, problem_detail_response)
    ```
    """
    # start with the correct headers and status code from the error
    response = e.get_response()
    # replace the body with JSON
    if isinstance(e, ProblemDetailError):
        response.data = json.dumps(e.as_problem_detail())
    else:
        response.data = json.dumps({'status': e.code, 'title': e.name, 'details': e.description})
    response.content_type = 'application/problem+json'
    return response
