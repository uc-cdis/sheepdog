sudo service postgresql stop
sudo service postgresql start 9.4
sudo -u postgres createuser -s -p 5432 travis
sudo -u postgres createdb -O travis -p 5432 travis
sudo -u postgres createuser -s -p 5433 travis
sudo -u postgres createdb -O travis -p 5433 travis

source ~/virtualenv/python2.7/bin/activate

python setup.py develop

psql -c "create database sheepdog_automated_test" -U postgres
pip freeze
python bin/setup_test_database.py
mkdir -p tests/integration/resources/keys; cd tests/integration/resources/keys; openssl genrsa -out test_private_key.pem 2048; openssl rsa -in test_private_key.pem -pubout -out test_public_key.pem; cd -

# commands to run tests
# datadict and datadictwithobjid tests must run separately to allow
# loading different datamodels
py.test -vv tests/integration/datadict
py.test -vv tests/integration/datadictwithobjid
py.test -vv tests/unit
