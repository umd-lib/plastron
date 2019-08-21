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

| Option                      |Description|
|-----------------------------|-----------|
|`SERVER`                     |The hostname and port of the STOMP server, e.g. `localhost:61613`|
|`EXPORT_JOBS_QUEUE`          |The name of the queue to subscribe to for receiving export job requests|
|`EXPORT_JOBS_COMPLETED_QUEUE`|The name of the queue to publish to when an export job is complete|

[STOMP]: https://stomp.github.io/
