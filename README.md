# Plastron

Utility for batch operations on a Fedora 4 repository.

## Installation

Requires Python 3.

```
**TODO**
add end-user instructions here once this is available via PyPI/pip
**TODO**
```

### Installation for development

To install Plastron in [development mode], do the following:

```
git clone git@github.com:umd-lib/plastron.git
cd plastron
pip install -e .
```

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
2.0.0.dev1
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

## Configuration

### Configuration Templates
Templates for creating the configuration files can be found at [config/templates](./config/templates)

### Repository Configuration

The repository connection is configured in a YAML file and passed to `plastron`
with the `-r` or `--repo` option. These are the recognized configuration keys:

#### Required

| Option        | Description |
| ------------- | ----------- |
|`REST_ENDPOINT`|Respository root URL|
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

## Extending

### Adding Commands

Commands are implemented as a package in `commands.{cmd_name}` that contain, at a
minimum, a class name `Command`. This class must have an `__init__` method that
takes an [argparse subparsers object] and creates and configures a subparser to
handle its specific command-line arguments. It must also have a `__call__` method
that takes a `pcdm.Repository` object and an [argparse.Namespace] object, and
executes the actual command.

To be enabled, the module name must be added to the `__all__` list in
[`commands/__init__.py`](commands/__init__.py).

For a simple example, see the ping command, as implemented in
[`commands/ping.py`](commands/ping.py):

```python
from classes.exceptions import FailureException

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

## License

See the [LICENSE](LICENSE.md) file for license rights and limitations (Apache 2.0).

[development mode]: https://packaging.python.org/tutorials/installing-packages/#installing-from-vcs
[argparse subparsers object]: https://docs.python.org/3/library/argparse.html#sub-commands
[argparse.Namespace]: https://docs.python.org/3/library/argparse.html#the-namespace-object
