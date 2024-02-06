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

### Running with Docker Swarm

This repository contains a [compose.yml](compose.yml) file that defines 
part of a `plastrond` Docker stack intended to be run alongside the
[umd-fcrepo-docker] stack. This repository's configuration adds a
`plastrond-http` container.

```bash
# if you are not already running in swarm mode
docker swarm init

# build the image
docker build -t docker.lib.umd.edu/plastrond-http:latest \
    -f plastron-web/Dockerfile .

# Copy the docker-plastron-template.yml and edit the configuration
cp docker-plastron.template.yml docker-plastron.yml
vim docker-plastron.yml

# deploy the stack to run the HTTP webapp
docker stack deploy -c plastron-web/compose.yml plastrond
```

To watch the logs:

```bash
docker service logs -f plastrond_http
```

To stop the HTTP service:

```bash
docker service rm plastrond_http
```

## Configuration

The application is configured through environment variables.

| Name       | Value                                      | Default |
|------------|--------------------------------------------|---------|
| `JOBS_DIR` | Root directory for storing job information | `jobs`  |

[umd-fcrepo-docker]: https://github.com/umd-lib/umd-fcrepo-docker
[Waitress]: https://pypi.org/project/waitress/
