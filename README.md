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
usage: load.py [-h] -r REPO -b BATCH [-d] [-n] [-l LIMIT] [-p] [-v] [-q]

A configurable batch loader for Fedora 4.

optional arguments:
  -h, --help            show this help message and exit
  -r REPO, --repo REPO  Path to repository configuration file.
  -b BATCH, --batch BATCH
                        Path to batch configuration file.
  -d, --dryrun          Iterate over the batch without POSTing.
  -n, --nobinaries      Iterate without uploading binaries.
  -l LIMIT, --limit LIMIT
                        Limit the load to a specified number of top-level
                        objects.
  -p, --ping            Check the connection to the repository and exit.
  -v, --verbose         Increase the verbosity of the status output.
  -q, --quiet           Decrease the verbosity of the status output.
```

## Create Collection

```
usage: create_collection.py [-h] -r REPO -n COLLECTION_NAME

Collection creation tool for Fedora 4.

Required arguments:
  -r REPO, --repo REPO  Path to repository configuration file.
  -n NAME, --name NAME  Name of the collection.

Optional arguments:
  -h, --help            show help message and exit
```

### Configuration Templates
Templates for creating the configuration files can be found at [config/templates](./config/templates)