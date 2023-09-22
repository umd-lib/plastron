# Plastron Testing

Plastron uses the [pytest] test framework for its [tests](../plastron-utils/tests).

## Basic Testing

Ensure that you have all the [modules](../plastron-utils/requirements.test.txt) required for
testing installed:

```bash
pip install -r requirements.test.txt
```

Run pytest:

```bash
pytest
```

## Advanced Testing

The Plastron project also includes a [tox.ini](../plastron-utils/tox.ini) configuration file
for running the test suite against multiple versions of Python. By default, it
will test against Python 3.6, 3.7, 3.8, 3.9, and 3.10.

The recommended way to ensure you have all those versions available is to use
[pyenv] to install the latest patch level of each of those minor versions. As
of this writing, that would be:

```bash
pyenv install 3.6.15
pyenv install 3.7.13
pyenv install 3.8.13
pyenv install 3.9.13
pyenv install 3.10.5
```

Ensures that the `python3.X` shims will all work by setting the local python 
versions:

```bash
pyenv local 3.10.5 3.9.13 3.8.13 3.7.13 3.6.15
```

Install [tox] and the [tox-pyenv] plugin:

```bash
pip install tox tox-pyenv
```

Run `tox` to test across multiple Python versions:

```bash
# use --recreate to create fresh virtualenvs
tox --recreate
```

To test against a single version of Python, supply an `-e` argument:

```bash
# only test against Python 3.8
tox -e py38
```

[pytest]: https://pypi.org/project/pytest/
[pyenv]: https://github.com/pyenv/pyenv
[tox]: https://pypi.org/project/tox/
[tox-pyenv]: https://pypi.org/project/tox-pyenv/
