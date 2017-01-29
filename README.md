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
usage: load.py [-h] -c CONFIG -H HANDLER [-m MAP] [-d] [-n] [-l LIMIT] [-p]
               [-x EXTRA] [-v] [-q]
               path

A configurable batch loader for Fedora 4.

positional arguments:
  path                  Path to data set to be loaded.

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        Path to configuration file.
  -H HANDLER, --handler HANDLER
                        Data handler module to use.
  -m MAP, --map MAP     Mapfile to store results of load.
  -d, --dryrun          Iterate over the batch without POSTing.
  -n, --nobinaries      Iterate without uploading binaries.
  -l LIMIT, --limit LIMIT
                        Limit the load to a specified number of top-level
                        objects.
  -p, --ping            Check the connection to the repository and exit.
  -x EXTRA, --extra EXTRA
                        File containing extra triples to add to each item
  -v, --verbose         Increase the verbosity of the status output.
  -q, --quiet           Decrease the verbosity of the status output.
```
