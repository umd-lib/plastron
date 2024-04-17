# Configuration

Plastron is configured using a YAML config file. The is passed to the CLI or
daemon process through a command line option: `-c` or `--config`.

## `REPOSITORY` section

### Required

| Option          | Description                               |
|-----------------|-------------------------------------------|
| `REST_ENDPOINT` | Repository root URL                       |
| `RELPATH`       | Path within repository to load objects to |
| `LOG_DIR`       | Directory to write log files              |

### JSON Web Token (JWT) Authentication

Only **one** of these should be used. `AUTH_TOKEN` takes precedence over
`JWT_SECRET` if both are present.

| Option       | Description                                                               |
|--------------|---------------------------------------------------------------------------|
| `AUTH_TOKEN` | Serialized JWT ready to be added to an "Authorization: Bearer ..." header |
| `JWT_SECRET` | Secret string to use to generate JWTs on-the-fly                          |

### Client Certificate Authentication

| Option        | Description                                    |
|---------------|------------------------------------------------|
| `CLIENT_CERT` | PEM-encoded client SSL cert for authentication |
| `CLIENT_KEY`  | PEM-encoded client SSL key for authentication  |

### Password Authentication

| Option            | Description                 |
|-------------------|-----------------------------|
| `FEDORA_USER`     | Username for authentication |
| `FEDORA_PASSWORD` | Password for authentication |

### Optional

| Option              | Description                                                                                                                                    |
|---------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| `SERVER_CERT`       | Path to a PEM-encoded copy of the server's SSL certificate; only needed for servers using self-signed certs                                    |
| `REPO_EXTERNAL_URL` | The URL to use for generating resource URIs, in preference to `REST_ENDPOINT`. Typically the "FCREPO_BASE_URL" parameter used with Kubernetes. |

## `MESSAGE_BROKER` section

This section configures the [STOMP] message broker (e.g., ActiveMQ).

| Option              | Description                                                   |
|---------------------|---------------------------------------------------------------|
| `SERVER`            | Hostname and port of the STOMP server, e.g. `localhost:61613` |
| `MESSAGE_STORE_DIR` | Path to the directory to hold the message inbox and outbox    |
| `DESTINATIONS`      | Sub-section containing queue and topic names                  |

### `DESTINATIONS` sub-section

This subsection configures the queues and topics used.

| Option         | Description                                                           |
|----------------|-----------------------------------------------------------------------|
| `JOBS`         | Name of the queue to subscribe to for receiving job requests          |
| `JOB_PROGRESS` | Name of the topic to publish job progress updates to for running jobs |
| `JOB_STATUS`   | Name of the queue to publish to job status updates to                 |
| `REINDEXING`   | Name of the queue to send requests for reindexing certain resources   |

## `COMMANDS` section

This section configures options for specific commands.

### `EXPORT` sub-section

Options for the export command:

| Option            | Description                                                     |
|-------------------|-----------------------------------------------------------------|
| `SSH_PRIVATE_KEY` | Filename of private key to use when making SSH/SFTP connections |

### `IMPORT` subsection

Options for the [import command](../plastron-cli/docs/import.md):

| Option            | Description                                                     |
|-------------------|-----------------------------------------------------------------|
| `SSH_PRIVATE_KEY` | Filename of private key to use when making SSH/SFTP connections |

## `SOLR` section

This section configures the connection to Solr.

| Option | Description                                                                   |
|--------|-------------------------------------------------------------------------------|
| `URL`  | Address to connect to Solr in the form `http://localhost:{port}/solr/fedora4` |

## `PUBLICATION_WORKFLOW` section

This section contains settings required to communicate with the handle 
service to fetch, update, and mint handles when publishing resources.

| Option               | Description                                                                                      |
|----------------------|--------------------------------------------------------------------------------------------------|
| `HANDLE_ENDPOINT`    | URL for the [umd-handle] service                                                                 |
| `HANDLE_JWT_TOKEN`   | JSON Web Token for access to the handle service                                                  |
| `HANDLE_PREFIX`      | Handle prefix identifier (UMD's is `1903.1`)                                                     |
| `HANDLE_REPO`        | Handle service name for the repository type (for Plastron, this should always be `fcrepo`)       |
| `PUBLIC_URL_PATTERN` | URI template for generating a public URL from an fcrepo URL; may contain a `{uuid}` placeholder. | 


[STOMP]: https://stomp.github.io/
[umd-handle]: https://github.com/umd-lib/umd-handle
