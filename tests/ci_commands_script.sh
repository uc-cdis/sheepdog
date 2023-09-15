#!/usr/bin/env bash

# since this whole thing is run as a bash {{this script}}, only the last pytest
# command controls the exit code. We actually want to exit early if something fails
set -e

# datadict and datadictwithobjid tests must run separately to allow
# loading different datamodels
poetry run pytest -vv --cov=sheepdog --cov-report xml tests/integration/datadict
poetry run pytest -vv --cov=sheepdog --cov-report xml --cov-append tests/integration/datadictwithobjid
poetry run pytest -vv --cov=sheepdog --cov-report xml --cov-append tests/unit