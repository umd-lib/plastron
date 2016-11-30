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
# get help
./load.py --help

# dry-run (run through process but do not touch repository)
./load.py -d -c config.yml -H ndnp /path/to/batch.xml

# no binaries (create and update only rdfsources)
./load.py -n -c config.yml -H ndnp /path/to/batch.xml

# do the load
./load.py -c config.yml -H ndnp /path/to/batch.xml
```
