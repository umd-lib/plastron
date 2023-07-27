# Plastron Server

The Plastron server (a.k.a., "plastrond") is implemented by the
[plastron.daemon](../src/plastron/daemon.py) module, and there is a 
*plastrond* entry point provided by [pyproject.toml](../pyproject.toml).

## Running with Python

```bash
# Install dependencies
pip install -e .
```

The Plastron Daemon itself can run in one of two modes:

* As a STOMP client: `plastrond -c <config file> stomp`
* An HTTP server: `plastrond -c <config file> http`

## Running with Docker Swarm

This repository contains a [docker-compose.yml](../docker-compose.yml) file 
that defines a Docker stack that can be run alongside the [umd-fcrepo-docker]
stack. This stack runs two containers, `plastrond-stomp` and `plastrond-http`,
each running the Plastron Daemon in their respective mode.

```
# if you are not already running in swarm mode
docker swarm init

# build the image
docker build -t plastrond .

# Create an "archelon_id" private/public key pair
# The Archelon instance should be configured with "archelon_id.pub" as the
# PLASTRON_PUBLIC_KEY
ssh-keygen -q -t rsa -N '' -f archelon_id

# Copy the docker-plastron-template.yml and edit the configuration
cp docker-plastron-template.yml docker-plastron.yml
vim docker-plastron.yml

# deploy the stack
docker stack deploy -c docker-compose.yml plastrond
```

To watch the logs:

```
# STOMP
docker service logs -f plastrond_plastrond-stomp

# HTTP
docker service logs -f plastrond_plastrond-http
```

## Configuration

See [Configuration](configuration.md).

See [docker-plastron-template.yml](../docker-plastron-template.yml) for an 
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

See the [messages documentation](messages.md) for details on the headers and bodies
of the messages the Plastron Daemon emits.

[umd-fcrepo-docker]: https://github.com/umd-lib/umd-fcrepo-docker
