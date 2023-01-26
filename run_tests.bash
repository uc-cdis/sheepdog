poetry install

psql -c "create database sheepdog_automated_test" -U postgres
python bin/setup_test_database.py
mkdir -p tests/integration/resources/keys; cd tests/integration/resources/keys; openssl genrsa -out test_private_key.pem 2048; openssl rsa -in test_private_key.pem -pubout -out test_public_key.pem; cd -

# commands to run tests
# datadict and datadictwithobjid tests must run separately to allow
# loading different datamodels
py.test -vv tests/integration/datadict
py.test -vv tests/integration/datadictwithobjid
py.test -vv tests/unit
