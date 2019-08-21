# Plastron

Utility for batch operations on a Fedora 4 repository.

## Installation

Requires Python 3.6+

```
**TODO**
add end-user instructions here once this is available via PyPI/pip
**TODO**
```

### Installing Python 3 with pyenv (Optional)

If you don't already have a Python 3 environment, or would like to install Plastron
into its own isolated environment, a very convenient way to do this is to use the
[pyenv] Python version manager.

See these instructions for [installing pyenv](https://github.com/pyenv/pyenv#installation),
then run the following:

```
# install Python 3.6.2
pyenv install 3.6.2

# create a new virtual environment based on 3.6.2 for Plastron
pyenv virtualenv 3.6.2 plastron

# switch to that environment in your current shell
pyenv shell plastron
```

### Installation for development

To install Plastron in [development mode], do the following:

```
git clone git@github.com:umd-lib/plastron.git
cd plastron
pip install -e .
```

## Running

* [Command-line client](docs/cli.md) ([plastron.cli](plastron/cli.py))
* [Server](docs/daemon.md) ([plastron.daemon](plastron/daemon.py))

## License

See the [LICENSE](LICENSE.md) file for license rights and limitations (Apache 2.0).

[pyenv]: https://github.com/pyenv/pyenv
[development mode]: https://packaging.python.org/tutorials/installing-packages/#installing-from-vcs
