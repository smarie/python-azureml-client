# python-azmlclient

*An ***unofficial*** generic client stack for azureML web services, working with both python 2 and 3.*

[![Python versions](https://img.shields.io/pypi/pyversions/azmlclient.svg)](https://pypi.python.org/pypi/azmlclient/) [![Build Status](https://github.com/smarie/python-azureml-client/actions/workflows/base.yml/badge.svg)](https://github.com/smarie/python-azureml-client/actions/workflows/base.yml) [![Tests Status](https://smarie.github.io/python-azureml-client/reports/junit/junit-badge.svg?dummy=8484744)](https://smarie.github.io/python-azureml-client/reports/junit/report.html) [![codecov](https://codecov.io/gh/smarie/python-azureml-client/branch/master/graph/badge.svg)](https://codecov.io/gh/smarie/python-azureml-client)

[![Documentation](https://img.shields.io/badge/doc-latest-blue.svg)](https://smarie.github.io/python-azureml-client/) [![PyPI](https://img.shields.io/pypi/v/azmlclient.svg)](https://pypi.python.org/pypi/azmlclient/) [![Downloads](https://pepy.tech/badge/azmlclient)](https://pepy.tech/project/azmlclient) [![Downloads per week](https://pepy.tech/badge/azmlclient/week)](https://pepy.tech/project/azmlclient) [![GitHub stars](https://img.shields.io/github/stars/smarie/python-azureml-client.svg)](https://github.com/smarie/python-azureml-client/stargazers)

**This is the readme for developers.** The documentation for users is available here: [https://smarie.github.io/python-azureml-client/](https://smarie.github.io/python-azureml-client/)

## Want to contribute ?

Contributions are welcome ! Simply fork this project on github, commit your contributions, and create pull requests.

Here is a non-exhaustive list of interesting open topics: [https://github.com/smarie/python-azureml-client/issues](https://github.com/smarie/python-azureml-client/issues)

## Installing all requirements

In order to install all requirements, including those for tests and packaging, use the following command:

```bash
pip install -r ci_tools/requirements-pip.txt
```

## Running the tests

This project uses `pytest`.

```bash
pytest -v azmlclient/tests/
```

## Packaging

This project uses `setuptools_scm` to synchronise the version number. Therefore the following command should be used for development snapshots as well as official releases: 

```bash
python setup.py egg_info bdist_wheel rotate -m.whl -k3
```

## Generating the documentation page

This project uses `mkdocs` to generate its documentation page. Therefore building a local copy of the doc page may be done using:

```bash
mkdocs build -f docs/mkdocs.yml
```

## Generating the test reports

The following commands generate the html test report and the associated badge. 

```bash
pytest --junitxml=junit.xml -v azmlclient/tests/
ant -f ci_tools/generate-junit-html.xml
python ci_tools/generate-junit-badge.py
```

### PyPI Releasing memo

This project is now automatically deployed to PyPI when a tag is created. Anyway, for manual deployment we can use:

```bash
twine upload dist/* -r pypitest
twine upload dist/*
```

### Merging pull requests with edits - memo

Ax explained in github ('get commandline instructions'):

```bash
git checkout -b <git_name>-<feature_branch> master
git pull https://github.com/<git_name>/python-azureml-client.git <feature_branch> --no-commit --ff-only
```

if the second step does not work, do a normal auto-merge (do not use **rebase**!):

```bash
git pull https://github.com/<git_name>/python-azureml-client.git <feature_branch> --no-commit
```

Finally review the changes, possibly perform some modifications, and commit.
