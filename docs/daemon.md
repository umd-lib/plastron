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

# create secrets from the SSL client and server certs;
# assume that $FCREPO_VAGRANT and $PLASTRON are your
# fcrepo-vagrant and plastron source code directories
cd $FCREPO_VAGRANT
bin/clientcert batchloader $PLASTRON/batchloader
cd $PLASTRON
docker secret create batchloader.pem batchloader.pem 
docker secret create batchloader.key batchloader.key 
docker secret create repository.pem $FCREPO_VAGRANT/dist/fcrepo/ssl/crt/fcrepolocal.crt 

# build the image
docker build -t plastrond .

# deploy the stack
docker stack deploy -c docker-compose.yml plastrond
```

To watch the logs:

```
docker logs -f plastrond_plastrond.1.<generated_id>
```

## Configuration

The plastrond configuration file is similar in format to the
[CLI configuration](cli.md), with the notable exception that it
introduces sections to separate the various systems in the
configuration.

See [docker-plastron.yml](../docker-plastron.yml) for an example
of the config file.

### `REPOSITORY` section

Options in this section are identical to those in the [CLI configuration](cli.md).

### `MESSAGE_BROKER` section

This section configures the [STOMP] message broker (e.g., ActiveMQ).

| Option            |Description|
|-------------------|-----------|
|`SERVER`           |Hostname and port of the STOMP server, e.g. `localhost:61613`|
|`MESSAGE_STORE_DIR`|Path to the directory to hold the message inbox and outbox|
|`DESTINATIONS`     |Sub-section containing queue and topic names|

#### `DESTINATIONS` sub-section

This sub-section configures the queues and topics used.

| Option         |Description|
|----------------|-----------|
|`JOBS`          |Name of the queue to subscribe to for receiving job requests|
|`JOB_STATUS`    |Name of the topic to publish status updates to for running jobs|
|`JOBS_COMPLETED`|Name of the queue to publish to when a job is complete|

### `COMMANDS` section

This section configures options for specific commands.

#### `EXPORT` sub-section

Options for the export command.

| Option          |Description|
|-----------------|-----------|
|`SSH_PRIVATE_KEY`|Filename of private key to use when making SSH/SFTP connections|

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

[STOMP]: https://stomp.github.io/
