# plastron-web

HTTP server for synchronous remote operations

## Running with Python

As a Flask application: 

```bash
flask --app plastron.web:create_app("/path/to/docker-plastron.yml") run
```

To enable debugging, for hot code reloading, set `FLASK_DEBUG=1` either on 
the command line or in a `.env` file:

```bash
FLASK_DEBUG=1 flask --app plastron.web:create_app("/path/to/docker-plastron.yml") run
```

Using the console script entrypoint, which runs the application with the 
[Waitress] WSGI server:

```bash
plastrond-http
```

## Docker Image

The plastron-stomp package contains a [Dockerfile](Dockerfile) for
building the `plastrond-http` Docker image.

### Building

**Important:** This image **MUST** be built from the main _plastron_
project directory, in order to include the other plastron packages in the
build context.

```bash
docker build -t docker.lib.umd.edu/plastrond-http:latest \
    -f plastron-web/Dockerfile .
```

## Configuration

The application is configured through environment variables.

| Name       | Value                                      | Default |
|------------|--------------------------------------------|---------|
| `JOBS_DIR` | Root directory for storing job information | `jobs`  |

[umd-fcrepo-docker]: https://github.com/umd-lib/umd-fcrepo-docker
[Waitress]: https://pypi.org/project/waitress/
