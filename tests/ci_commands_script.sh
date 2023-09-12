#!/usr/bin/env bash
# datadict and datadictwithobjid tests must run separately to allow
# loading different datamodels
pytest -vv --cov=sheepdog --cov-report xml tests/integration/datadict
pytest -vv --cov=sheepdog --cov-report xml --cov-append tests/integration/datadictwithobjid
pytest -vv --cov=sheepdog --cov-report xml --cov-append tests/unit