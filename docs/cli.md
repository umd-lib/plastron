# Plastron Command-line Client

## Common Options

```
$ plastron --help
usage: plastron [-h] (-r REPO | -V) [-v] [-q] [--on-behalf-of DELEGATED_USER]
                {delete,del,rm,export,extractocr,imgsize,import,list,ls,load,mkcol,ping,update}
                ...

Batch operation tool for Fedora 4.

optional arguments:
  -h, --help            show this help message and exit
  -r REPO, --repo REPO  Path to repository configuration file.
  -V, --version         Print version and exit.
  -v, --verbose         increase the verbosity of the status output
  -q, --quiet           decrease the verbosity of the status output
  --on-behalf-of DELEGATED_USER
                        delegate repository operations to this username

commands:
  {delete,del,rm,export,extractocr,imgsize,import,list,ls,load,mkcol,ping,update}
```

### Check version

```
$ plastron --version
3.2.0rc2
```

## Commands

All commands require you to specify a repository configuration file using
the `-r` or `--repo` option *before* the command name. For example,
`plastron -r path/to/repo.yml ping`.

### Ping (ping)

```
$ plastron ping --help
usage: plastron ping [-h]

Check connection to the repository

optional arguments:
  -h, --help  show this help message and exit
```

### Load (load)

```
$ plastron load --help
usage: plastron load [-h] -b BATCH [-d] [-n] [-l LIMIT] [-% PERCENT]
                     [--no-annotations] [--no-transactions] [--ignore IGNORE]
                     [--wait WAIT]

Load a batch into the repository

optional arguments:
  -h, --help            show this help message and exit
  -d, --dry-run         iterate over the batch without POSTing
  -n, --no-binaries     iterate without uploading binaries
  -l LIMIT, --limit LIMIT
                        limit the load to a specified number of top-level
                        objects
  -% PERCENT, --percent PERCENT
                        load specified percentage of total items
  --no-annotations      iterate without loading annotations (e.g. OCR)
  --no-transactions, --no-txn
                        run the load without using transactions
  --ignore IGNORE, -i IGNORE
                        file listing items to ignore
  --wait WAIT, -w WAIT  wait n seconds between items

required arguments:
  -b BATCH, --batch BATCH
                        path to batch configuration file
```

### List (list, ls)

```
$ plastron list --help
usage: plastron list [-h] [-l] [-R RECURSIVE] [uris [uris ...]]

List objects in the repository

positional arguments:
  uris                  URIs of repository objects to list

optional arguments:
  -h, --help            show this help message and exit
  -l, --long            Display additional information besides the URI
  -R RECURSIVE, --recursive RECURSIVE
                        List additional objects found by traversing the given
                        predicate(s)
```

### Create Collection (mkcol)

```
$ plastron mkcol --help
usage: plastron mkcol [-h] -n NAME [-b BATCH] [--notransactions]

Create a PCDM Collection in the repository

optional arguments:
  -h, --help            show this help message and exit
  -n NAME, --name NAME  Name of the collection.
  -b BATCH, --batch BATCH
                        Path to batch configuration file.
  --notransactions      run the load without using transactions
```

### Delete (delete, del, rm)

```
$ plastron delete --help
usage: plastron delete [-h] [-R RECURSIVE] [-d] [--no-transactions]
                       [--completed COMPLETED] [-f FILE]
                       [uris [uris ...]]

Delete objects from the repository

positional arguments:
  uris                  Repository URIs to be deleted.

optional arguments:
  -h, --help            show this help message and exit
  -R RECURSIVE, --recursive RECURSIVE
                        Delete additional objects found by traversing the
                        given predicate(s)
  -d, --dry-run         Simulate a delete without modifying the repository
  --no-transactions, --no-txn
                        run the update without using transactions
  --completed COMPLETED
                        file recording the URIs of deleted resources
  -f FILE, --file FILE  File containing a list of URIs to delete```
```

### Extract OCR (extractocr)

```
$ plastron extractocr --help
usage: plastron extractocr [-h] [--ignore IGNORE]

Create annotations from OCR data stored in the repository

optional arguments:
  -h, --help            show this help message and exit
  --ignore IGNORE, -i IGNORE
                        file listing items to ignore
```

### Export (export)

```
$ plastron export --help
usage: plastron export [-h] --output-dest OUTPUT_DEST [--key KEY] -f
                       {text/turtle,turtle,ttl,text/csv,csv}
                       [--uri-template URI_TEMPLATE] [--export-binaries]
                       [--binary-types BINARY_TYPES]
                       [uris [uris ...]]

Export resources from the repository as a BagIt bag

positional arguments:
  uris                  URIs of repository objects to export

optional arguments:
  -h, --help            show this help message and exit
  --output-dest OUTPUT_DEST
                        Where to send the export. Can be a local filename or
                        an SFTP URI
  --key KEY             SSH private key file to use for SFTP connections
  -f {text/turtle,turtle,ttl,text/csv,csv}, --format {text/turtle,turtle,ttl,text/csv,csv}
                        Format for exported metadata
  --uri-template URI_TEMPLATE
                        Public URI template
  --export-binaries     Export binaries in addition to the metadata
  --binary-types BINARY_TYPES
                        Include only binaries with a MIME type from this list
```

### Update (update)

```
$ plastron update --help
usage: plastron update [-h] -u UPDATE_FILE [-R RECURSIVE] [-d]
                       [--no-transactions] [--validate] [-m MODEL]
                       [--completed COMPLETED] [-f FILE]
                       [uris [uris ...]]

Update objects in the repository

positional arguments:
  uris                  URIs of repository objects to update

optional arguments:
  -h, --help            show this help message and exit
  -u UPDATE_FILE, --update-file UPDATE_FILE
                        Path to SPARQL Update file to apply
  -R RECURSIVE, --recursive RECURSIVE
                        Update additional objects found by traversing the
                        given predicate(s)
  -d, --dry-run         Simulate an update without modifying the repository
  --no-transactions, --no-txn
                        run the update without using transactions
  --validate            validate before updating
  -m MODEL, --model MODEL
                        The model class to use for validation (Item, Issue,
                        Poster, or Letter)
  --completed COMPLETED
                        file recording the URIs of updated resources
  -f FILE, --file FILE  File containing a list of URIs to update
```

### Import (import)

```
$ plastron import --help
usage: plastron import [-h] -m MODEL [-l LIMIT] [--validate-only]
                       [--make-template TEMPLATE_FILE] [--access ACCESS]
                       [--member-of MEMBER_OF]
                       [--binaries-location BINARIES_LOCATION]
                       [import_file]

Import data to the repository

positional arguments:
  import_file           name of the file to import from

optional arguments:
  -h, --help            show this help message and exit
  -m MODEL, --model MODEL
                        data model to use
  -l LIMIT, --limit LIMIT
                        limit the number of rows to read from the import file
  --validate-only       only validate, do not do the actual import
  --make-template TEMPLATE_FILE
                        create a CSV template for the given model
  --access ACCESS       URI or CURIE of the access class to apply to new items
  --member-of MEMBER_OF
                        URI of the object that new items are PCDM members of
  --binaries-location BINARIES_LOCATION
                        where to find binaries; either a path to a directory,
                        a "zip:<path to zipfile>" URI, an SFTP URI in the form
                        "sftp://<user>@<host>/<path to dir>", or a URI in the
                        form "zip+sftp://<user>@<host>/<path to zipfile>"
```

### Echo (echo)

```
$ plastron echo --help
usage: plastron echo [-h] [-e ECHO_DELAY] -b BODY

Diagnostic command for echoing input to output. Primarily intended for testing
synchronous message processing.

optional arguments:
  -h, --help            show this help message and exit
  -e ECHO_DELAY, --echo-delay ECHO_DELAY
                        The amount of time to delay the reply, in seconds
  -b BODY, --body BODY  The text to echo back
```

## Configuration

### Configuration Templates

Templates for creating the configuration files can be found at [config/templates](../config/templates)

### Repository Configuration

The repository connection is configured in a YAML file and passed to `plastron`
with the `-r` or `--repo` option. These are the recognized configuration keys:

#### Required

| Option        | Description |
| ------------- | ----------- |
|`REST_ENDPOINT`|Repository root URL|
|`RELPATH`      |Path within repository to load objects to|
|`LOG_DIR`      |Directory to write log files|

#### Client Certificate Authentication

| Option      | Description |
| ----------- | ----------- |
|`CLIENT_CERT`|PEM-encoded client SSL cert for authentication|
|`CLIENT_KEY` |PEM-encoded client SSL key for authentication|

#### Password Authentication

| Option          | Description |
| --------------- | ----------- |
|`FEDORA_USER`    |Username for authentication|
|`FEDORA_PASSWORD`|Password for authentication|

#### Optional

| Option      | Description |
| ----------- | ----------- |
|`SERVER_CERT`|Path to a PEM-encoded copy of the server's SSL certificate; only needed for servers using self-signed certs|

### Batch Configuration

#### Required

| Option | Description |
| ------ | ----------- |
|`BATCH_FILE`|The "main" file of the batch|
|`COLLECTION`|URI of the repository collection that the objects will be added to|
|`HANDLER`|The handler to use|

#### Optional

| Option | Description | Default |
| ------ | ----------- | ------- |
|`ROOT_DIR`| |The directory containing the batch configuration file|
|`DATA_DIR`|Where to find the data files for the batch; relative paths are relative to `ROOT_DIR`|`data`|
|`LOG_DIR`|Where to write the mapfile, skipfile, and other logging info; relative paths are relative to `ROOT_DIR`|`logs`|
|`MAPFILE`|Where to store the record of completed items in this batch; relative paths are relative to `LOG_DIR`|`mapfile.csv`|
|`HANDLER_OPTIONS`|Any additional options required by the handler| |

**Note:** The `plastron.load.*.log` files are currently written to the repository log directory, *not* to batch log directory.

## Extending

### Adding Commands

Commands are implemented as a package in `plastron.commands.{cmd_name}` that
contain, at a minimum, a class name `Command`. This class must have an `__init__`
method that takes an [argparse subparsers object] and creates and configures a
subparser to handle its specific command-line arguments. It must also have a
`__call__` method that takes a `pcdm.Repository` object and an [argparse.Namespace]
object, and executes the actual command.

For a simple example, see the ping command, as implemented in
[`plastron/commands/ping.py`](../plastron/commands/ping.py):

```python
from plastron.exceptions import FailureException

class Command:
    def __init__(self, subparsers):
        parser_ping = subparsers.add_parser('ping',
                description='Check connection to the repository')
        parser_ping.set_defaults(cmd_name='ping')

    def __call__(self, fcrepo, args):
        try:
            fcrepo.test_connection()
        except:
            raise FailureException()
```

The `FailureException` is caught by the `plastron` script and causes it to exit with
a status code of 1. Any `KeyboardInterrupt` exceptions (for instance, due to the
user pressing <kbd>Ctrl+C</kbd>) are also caught by the `plastron` script and cause
it to exit with a status code of 2.

[argparse subparsers object]: https://docs.python.org/3/library/argparse.html#sub-commands
[argparse.Namespace]: https://docs.python.org/3/library/argparse.html#the-namespace-object
