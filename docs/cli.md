# Plastron Command-line Client

## Common Options

```text
$ plastron --help
usage: plastron [-h] (-r REPO | -c CONFIG_FILE | -V) [-v] [-q]
                [--on-behalf-of DELEGATED_USER]
                {annotate,create,delete,del,rm,echo,export,extractocr,find,imgsize,import,list,ls,load,mkcol,ping,reindex,stub,update}
                ...

Batch operation tool for Fedora 4.

optional arguments:
  -h, --help            show this help message and exit
  -r REPO, --repo REPO  Path to repository configuration file.
  -c CONFIG_FILE, --config CONFIG_FILE
                        Path to configuration file.
  -V, --version         Print version and exit.
  -v, --verbose         increase the verbosity of the status output
  -q, --quiet           decrease the verbosity of the status output
  --on-behalf-of DELEGATED_USER
                        delegate repository operations to this username

commands:
  {annotate,create,delete,del,rm,echo,export,extractocr,find,imgsize,import,list,ls,load,mkcol,ping,reindex,stub,update}
```

### Check version

```bash
$ plastron --version
3.7.0
```

## Commands

All commands require you to specify a configuration file using either the
`-c|--config` or `-r|--repo` option *before* the command name. For example,
`plastron -c path/to/repo.yml ping`.

### Annotate (annotate)

```text
$ plastron annotate --help
usage: plastron annotate [-h] [uris [uris ...]]

Annotate resources with the text content of their HTML files

positional arguments:
  uris        URIs of repository objects to process

optional arguments:
  -h, --help  show this help message and exit
```

### Create (create)

```text
$ plastron create -h
usage: plastron create [-h] [-D PREDICATE VALUE] [-O PREDICATE VALUE]
                       [-T TYPE] [--collection NAME] [--container PATH]
                       [path]

Create a resource in the repository

positional arguments:
  path                  path to the new resource

optional arguments:
  -h, --help            show this help message and exit
  -D PREDICATE VALUE, --data-property PREDICATE VALUE
                        an RDF data property to set on the newly created
                        resource; VALUE is treated as a Literal; repeatable
  -O PREDICATE VALUE, --object-property PREDICATE VALUE
                        an RDF object property to set on the newly created
                        resource; VALUE is treated as a CURIE or URIRef;
                        repeatable
  -T TYPE, --rdf-type TYPE
                        RDF type to add to the newly created resource;
                        equivalent to "-O rdf:type TYPE"; TYPE is treated as a
                        CURIE or URIRef; repeatable
  --collection NAME     shortcut for "-T pcdm:collection -D dcterms:title
                        NAME"
  --container PATH      parent container for the new resource; use this to
                        create a new resource with a repository-generated
                        identifier
```

### Create Collection (mkcol)

**DEPRECATED:** Use [`plastron create --collection`](#create-create) instead.

```text
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

See [Delete Command](delete.md)

### Echo (echo)

```text
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

### Export (export)

```text
$ plastron export --help
usage: plastron export [-h] -o OUTPUT_DEST [--key KEY] -f
                       {text/turtle,turtle,ttl,text/csv,csv}
                       [--uri-template URI_TEMPLATE] [-B]
                       [--binary-types BINARY_TYPES]
                       [uris [uris ...]]

Export resources from the repository as a BagIt bag

positional arguments:
  uris                  URIs of repository objects to export

optional arguments:
  -h, --help            show this help message and exit
  -o OUTPUT_DEST, --output-dest OUTPUT_DEST
                        Where to send the export. Can be a local filename or
                        an SFTP URI
  --key KEY             SSH private key file to use for SFTP connections
  -f {text/turtle,turtle,ttl,text/csv,csv}, --format {text/turtle,turtle,ttl,text/csv,csv}
                        Format for exported metadata
  --uri-template URI_TEMPLATE
                        Public URI template
  -B, --export-binaries
                        Export binaries in addition to the metadata
  --binary-types BINARY_TYPES
                        Include only binaries with a MIME type from this list
```

### Extract OCR (extractocr)

```text
$ plastron extractocr --help
usage: plastron extractocr [-h] [--ignore IGNORE]

Create annotations from OCR data stored in the repository

optional arguments:
  -h, --help            show this help message and exit
  --ignore IGNORE, -i IGNORE
                        file listing items to ignore
```

### Find (find)

```text
$ plastron find --help
usage: plastron find [-h] [-R RECURSIVE] [-D PREDICATE VALUE]
                     [-O PREDICATE VALUE] [-T TYPE]
                     [--match-all | --match-any]
                     [URI [URI ...]]

Find objects in the repository

positional arguments:
  URI                   search at this URI in the repository

optional arguments:
  -h, --help            show this help message and exit
  -R RECURSIVE, --recursive RECURSIVE
                        search additional objects found by traversing the
                        given predicate(s)
  -D PREDICATE VALUE, --data-property PREDICATE VALUE
                        an RDF data property to match; VALUE is treated as a
                        Literal; repeatable
  -O PREDICATE VALUE, --object-property PREDICATE VALUE
                        an RDF object property to match; VALUE is treated as a
                        CURIE or URIRef; repeatable
  -T TYPE, --rdf-type TYPE
                        RDF type to match; equivalent to "-O rdf:type TYPE";
                        TYPE is treated as a CURIE or URIRef; repeatable
  --match-all           require all properties to match to include a resource
                        in the result list; this is the default behavior
  --match-any           require at least one property to match to include a
                        resource in the result list
```

### Image Size (imgsize)

```text
$ plastron imgsize --help
usage: plastron imgsize [-h] [uris [uris ...]]

Add width and height to image resources

positional arguments:
  uris        URIs of repository objects to get image info

optional arguments:
  -h, --help  show this help message and exit
```

### Import (import)

See [Import Command](import.md)

### List (list, ls)

```text
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

### Load (load)

```text
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

### Ping (ping)

```text
$ plastron ping --help
usage: plastron ping [-h]

Check connection to the repository

optional arguments:
  -h, --help  show this help message and exit
```

### Reindex (reindex)

```text
$ plastron reindex --help
usage: plastron reindex [-h] [-R RECURSIVE] [uris [uris ...]]

Reindex objects in the repository

positional arguments:
  uris                  URIs of repository objects to reindex

optional arguments:
  -h, --help            show this help message and exit
  -R RECURSIVE, --recursive RECURSIVE
                        Reindex additional objects found by traversing the
                        given predicate(s)
```

### Stub (stub)

```text
$ plastron stub --help
usage: plastron stub [-h] --identifier-column IDENTIFIER_COLUMN
                     --binary-column BINARY_COLUMN
                     [--rename-binary-column RENAME_BINARY_COLUMN]
                     [--member-of MEMBER_OF] [--access ACCESS]
                     [--container CONTAINER_PATH] [-o OUTPUT_FILE]
                     source_file

create stub resources with just an identifier and binary

positional arguments:
  source_file           name of the CSV file to create stubs from; use "-" to
                        read from STDIN

optional arguments:
  -h, --help            show this help message and exit
  --identifier-column IDENTIFIER_COLUMN
                        column in the source CSV file with a unique identifier
                        for each item
  --binary-column BINARY_COLUMN
                        column in the source CSV file with the location of the
                        binary to load. Supports http: and https: (must begin
                        with "http:" or "https:"), and file resources
                        (relative or absolute file path). Relative file paths
                        are relative to where the command is run.
  --rename-binary-column RENAME_BINARY_COLUMN
                        Renames the binary column in the CSV output to the
                        provided name.
  --member-of MEMBER_OF
                        URI of the object that new items are PCDM members of
  --access ACCESS       URI or CURIE of the access class to apply to new items
  --container CONTAINER_PATH
                        parent container for new items; defaults to the
                        RELPATH in the repo configuration file
  -o OUTPUT_FILE, --output-file OUTPUT_FILE
                        destination for a copy of the source CSV file with the
                        binary-column value replaced with the newly created
                        repository URI for the binary; defaults to STDOUT if
                        not given
```

### Update (update)

```text
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

## Configuration

### Repository Configuration

The repository connection is configured in a YAML file and passed to `plastron`
with the `-r` or `--repo` option. These are the recognized configuration keys:

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
