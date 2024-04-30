#!/usr/bin/env bash
set -e
poetry run python bin/setup_test_database.py
mkdir -p tests/integration/resources/keys
cd tests/integration/resources/keys
openssl genrsa -out test_private_key.pem 2048
openssl rsa -in test_private_key.pem -pubout -out test_public_key.pem
cd -
