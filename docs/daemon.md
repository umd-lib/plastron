# Plastron Server

The Plastron server (a.k.a., "plastrond") is implemented by the
[plastron.daemon](../plastron/daemon.py) module, and there is a
*plastrond* entry point provided by [setup.py](../setup.py).

## Running with Python

```
pip install -e .
plastrond -c <config_file>
```

## Running with Docker

```
# if you are not already running in swarm mode
docker swarm init

# build the image
docker build -t plastrond .

# Create an "archelon_id" private/public key pair
# The Archelon instance should be configured with "archelon_id.pub" as the
# PLASTRON_PUBLIC_KEY
ssh-keygen -q -t rsa -N '' -f archelon_id

# deploy the stack
docker stack deploy -c docker-compose.yml plastrond
```

To watch the logs:

```
docker logs -f plastrond_plastrond.1.<generated_id>
```

## Configuration

See [Configuration](configuration.md).

See [docker-plastron.yml](../docker-plastron.yml) for an example
of the config file.

## STOMP Message Headers

The Plastron Daemon expects the following headers to be present in messages
received on the `JOBS` destination:

* `PlastronCommand`
* `PlastronJobId`

Additional arguments for a command are sent in headers with the form `PlastronArg-{name}`.
Many of these are specific to the command, but there is one with standard behavior across
all commands:

| Header                   | Description |
|--------------------------|-------------|
|`PlastronArg-on-behalf-of`|Username to delegate repository operations to|

See the [messages documentation](messages.md) for details on the headers and bodies
of the messages the Plastron Daemon emits.
