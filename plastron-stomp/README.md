# plastron-stomp

STOMP listener client for asynchronous and synchronous operations

## Running with Python

As a Python module:

```bash
python -m plastron.stomp.daemon -c <config file>
```

Using the console script entrypoint:

```bash
plastrond-stomp -c <config file>
```

## Integration Tests

See the [integration test README](integration-tests/README.md) for
instructions on running the manual integration tests.

## Docker Image

The plastron-stomp package contains a [Dockerfile](Dockerfile) for 
building the `plastrond-stomp` Docker image.

### Building

**Important:** This image **MUST** be built from the main _plastron_ 
project directory, in order to include the other plastron packages in the 
build context.

```bash
docker build -t docker.lib.umd.edu/plastrond-stomp:latest \
    -f plastron-stomp/Dockerfile .
```

### Running with Docker Swarm

This repository contains a [compose.yml](compose.yml) file that defines 
part of a `plastrond` Docker stack intended to be run alongside the 
[umd-fcrepo-docker] stack. This repository's configuration adds a 
`plastrond-stomp` container.

```bash
# if you are not already running in swarm mode
docker swarm init

# build the image
docker build -t docker.lib.umd.edu/plastrond-stomp:latest \
    -f plastron-stomp/Dockerfile .

# Create an "archelon_id" private/public key pair
# The Archelon instance should be configured with "archelon_id.pub" as the
# PLASTRON_PUBLIC_KEY
ssh-keygen -q -t rsa -N '' -f archelon_id

# Copy the docker-plastron-template.yml and edit the configuration
cp docker-plastron.template.yml docker-plastron.yml
vim docker-plastron.yml

# deploy the stack to run the STOMP application
docker stack deploy -c plastron-stomp/compose.yml plastrond
```

To watch the logs:

```bash
docker service logs -f plastrond_stomp
```

To stop the STOMP service:

```bash
docker service rm plastrond_stomp
```

## Configuration

See [docker-plastron.template.yml](../docker-plastron.template.yml) for an
example of the config file.

## STOMP Message Headers

The Plastron Daemon expects the following headers to be present in messages
received on the `JOBS` destination:

* `PlastronCommand`
* `PlastronJobId`

Additional arguments for a command are sent in headers with the form `PlastronArg-{name}`.
Many of these are specific to the command, but there is one with standard behavior across
all commands:

| Header                     | Description                                   |
|----------------------------|-----------------------------------------------|
| `PlastronArg-on-behalf-of` | Username to delegate repository operations to |

See the [messages documentation](docs/messages.md) for details on the headers 
and bodies of the messages the Plastron STOMP Daemon emits.

[umd-fcrepo-docker]: https://github.com/umd-lib/umd-fcrepo-docker
