# Plastron Command-line Client

## Common Options

```
$ plastron --help
usage: plastron [-h] (-r REPO | -V) [-v] [-q]
                {ping,load,list,ls,mkcol,delete,del,rm,extractocr} ...

Batch operation tool for Fedora 4.

optional arguments:
  -h, --help            show this help message and exit
  -r REPO, --repo REPO  Path to repository configuration file.
  -V, --version         Print version and exit.
  -v, --verbose         increase the verbosity of the status output
  -q, --quiet           decrease the verbosity of the status output

commands:
  {ping,load,list,ls,mkcol,delete,del,rm,extractocr}
```

### Check version

```
$ plastron --version
3.0.0-dev
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
                     [--noannotations] [--ignore IGNORE] [--wait WAIT]

Load a batch into the repository

optional arguments:
  -h, --help            show this help message and exit
  -d, --dryrun          iterate over the batch without POSTing
  -n, --nobinaries      iterate without uploading binaries
  -l LIMIT, --limit LIMIT
                        limit the load to a specified number of top-level
                        objects
  -% PERCENT, --percent PERCENT
                        load specified percentage of total items
  --noannotations       iterate without loading annotations (e.g. OCR)
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
usage: plastron mkcol [-h] -n NAME [-b BATCH]

Create a PCDM Collection in the repository

optional arguments:
  -h, --help            show this help message and exit
  -n NAME, --name NAME  Name of the collection.
  -b BATCH, --batch BATCH
                        Path to batch configuration file.
```

### Delete (delete, del, rm)

```
$ plastron delete --help
usage: plastron delete [-h] [-R RECURSIVE] [-d] [-f FILE] [uris [uris ...]]

Delete objects from the repository

positional arguments:
  uris                  Repository URIs to be deleted.

optional arguments:
  -h, --help            show this help message and exit
  -R RECURSIVE, --recursive RECURSIVE
                        Delete additional objects found by traversing the
                        given predicate(s)
  -d, --dryrun          Simulate a delete without modifying the repository
  -f FILE, --file FILE  File containing a list of URIs to delete
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
usage: plastron export [-h] [-o OUTPUT_FILE] -f
                       {text/turtle,turtle,ttl,text/csv,csv}
                       [uris [uris ...]]

Export resources from the repository

positional arguments:
  uris                  URIs of repository objects to export

optional arguments:
  -h, --help            show this help message and exit
  -o OUTPUT_FILE, --output-file OUTPUT_FILE
                        File to write export package to
  -f {text/turtle,turtle,ttl,text/csv,csv}, --format {text/turtle,turtle,ttl,text/csv,csv}
                        Export job format
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
