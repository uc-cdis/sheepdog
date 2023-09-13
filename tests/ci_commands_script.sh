#!/usr/bin/env bash
# datadict and datadictwithobjid tests must run separately to allow
# loading different datamodels
poetry run pytest -vv --cov=sheepdog --cov-report xml tests/integration/datadict

# since this whole thing is run as a bash {{this script}}, only the last pytest
# command controls the exit code. We actually want to exit early if something fails,
# so check that here and exit as necessary
RESULT=$?
if [ $RESULT -ne 0 ]; then
  exit 1
fi

poetry run pytest -vv --cov=sheepdog --cov-report xml --cov-append tests/integration/datadictwithobjid

RESULT=$?
if [ $RESULT -ne 0 ]; then
  exit 1
fi

poetry run pytest -vv --cov=sheepdog --cov-report xml --cov-append tests/unit