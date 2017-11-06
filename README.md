Utility for batch loading newspaper content into a Fedora 4 repository.

## Installation

Requires Python 3.

```
git clone git@github.com:umd-lib/newspaper-batchload.git
cd newspaper-batchload
pip install -r requirements.txt
```

## Running

```
usage: load.py [-h] -r REPO -b BATCH [-d] [-n] [-l LIMIT] [-% PERCENT] [-p]
               [-v] [-q] [--noannotations] [--ignore IGNORE] [--wait WAIT]

A configurable batch loader for Fedora 4.

optional arguments:
  -h, --help            show this help message and exit
  -d, --dryrun          iterate over the batch without POSTing
  -n, --nobinaries      iterate without uploading binaries
  -l LIMIT, --limit LIMIT
                        limit the load to a specified number of top-level
                        objects
  -% PERCENT, --percent PERCENT
                        load specified percentage of total items
  -p, --ping            check the repo connection and exit
  -v, --verbose         increase the verbosity of the status output
  -q, --quiet           decrease the verbosity of the status output
  --noannotations       iterate without loading annotations (e.g. OCR)
  --ignore IGNORE, -i IGNORE
                        file listing items to ignore
  --wait WAIT, -w WAIT  wait n seconds between items

required arguments:
  -r REPO, --repo REPO  path to repository configuration file
  -b BATCH, --batch BATCH
                        path to batch configuration file
```

## Create Collection

```
usage: create_collection.py [-h] -r REPO -n NAME [-b BATCH]

Collection creation tool for Fedora 4.

optional arguments:
  -h, --help            show this help message and exit
  -r REPO, --repo REPO  Path to repository configuration file.
  -n NAME, --name NAME  Name of the collection.
  -b BATCH, --batch BATCH
                        Path to batch configuration file.
```

## Extract OCR

```
usage: extractocr.py [-h] -r REPO [--ignore IGNORE]

Extract OCR text and create annotations.

optional arguments:
  -h, --help            show this help message and exit
  -r REPO, --repo REPO  Path to repository configuration file.
  --ignore IGNORE, -i IGNORE
                        file listing items to ignore
```

## Object Lister

```
usage: list.py [-h] -r REPO [-l] [-R RECURSIVE]

Object lister for Fedora 4.

optional arguments:
  -h, --help            show this help message and exit
  -r REPO, --repo REPO  Path to repository configuration file.
  -l, --long            Display additional information besides the URI
  -R RECURSIVE, --recursive RECURSIVE
                        List additional objects found by traversing the given
                        predicate(s)
```

## Delete Tool

```
usage: delete.py [-h] -r REPO [-p] [-R RECURSIVE] [-d] [-f FILE]
                 [uris [uris ...]]

Delete tool for Fedora 4.

positional arguments:
  uris                  Zero or more repository URIs to be deleted.

optional arguments:
  -h, --help            show this help message and exit
  -r REPO, --repo REPO  Path to repository configuration file.
  -p, --ping            Check the connection to the repository and exit.
  -R RECURSIVE, --recursive RECURSIVE
                        Delete additional objects found by traversing the
                        given predicate(s)
  -d, --dryrun          Simulate a delete without modifying the repository
  -f FILE, --file FILE  File containing a list of URIs to delete
```

### Configuration Templates
Templates for creating the configuration files can be found at [config/templates](./config/templates)

## License

See the [LICENSE](LICENSE.md) file for license rights and limitations (Apache 2.0).

