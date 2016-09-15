Utility for batch loading newspaper content into a Fedora 4 repository.

## Installation

```
git clone git@github.com:jwestgard/newspaper-batchload.git
cd newspaper-batchload
pip install -r requirements.txt
```

## Running

```
# get help
./load.py --help

# dry-run
./load.py -d -c config.yml -H ndnp /path/to/batch.xml

# do the load
./load.py -c config.yml -H ndnp /path/to/batch.xml
```
