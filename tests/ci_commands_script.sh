#!/usr/bin/env bash
# datadict and datadictwithobjid tests must run separately to allow
# loading different datamodels
poetry run pytest -vv --cov=sheepdog --cov-report xml tests/integration/datadict
poetry run pytest -vv --cov=sheepdog --cov-report xml --cov-append tests/integration/datadictwithobjid
poetry run pytest -vv --cov=sheepdog --cov-report xml --cov-append tests/unit