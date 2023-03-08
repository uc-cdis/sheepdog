# Set up a local development environment

This guide will cover setting up a sheepdog development environment.

The cloud settings are still to be determined.

## Set up Working Directory

Clone the repo locally.

```console
git clone https://github.com/uc-cdis/sheepdog.git
```

Navigate to the cloned repository directory.

## Set up Python 3.9

You can use `bash` to install python 3 if it's not already available.

```console
sudo apt-get update
sudo apt-get install python3
```

### Set up a Virtual Environment

Set up a virtual environment for use with this project using `bash`:

```console
python3 -m venv py3-venv
. py3-venv/bin/activate
```

## Set up local Postgresql DB for testing

You can use a local postgresql for testing purposes.

### Set up local Postgresql DB on WSL

You can use `bash` to install postgres:

```console
sudo apt install postgresql-client-common
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt-get update
sudo apt-get install postgresql-12
```

Make sure the cluster is started:

```console
sudo pg_ctlcluster 12 main start
```

### Set up local Postgresql DB on Mac

If you're on mac, you can install postgres using brew:

```console
brew install postgres
```

### Set up DB and users for testing

You'll need to connect to the postgresql and add test users and databases.

#### Connect to Postgresql on WSL

Connect to the local postgresql server

```console
sudo -i -u postgres
psql
```

#### Connect to Postgresql on Mac

If you're on a mac, use the following to connect to postgres:

```console
brew services start postgres
psql postgres
```

#### Helpful psql commands
It may be helpful to understand some psql commands too:

```console
\conninfo # check connection info
\l # list databases
\d # list tables in database
\c # list short connection info
\c postgres # connect to a database named postgres
\q # quit
```

#### Set up users in psql

Initialize a user within the psql console:

```console
CREATE USER postgres WITH PASSWORD 'test';
ALTER USER postgres WITH PASSWORD 'test';
\du
```


### Quickstart with Helm

You can now deploy individual services via Helm! 

If you are looking to deploy all Gen3 services, that can be done via the Gen3 Helm chart. 
Instructions for deploying all Gen3 services with Helm can be found [here](https://github.com/uc-cdis/gen3-helm#readme).

To deploy the sheepdog service:
```bash
helm repo add gen3 https://helm.gen3.org
helm repo update
helm upgrade --install gen3/sheepdog
```
These commands will add the Gen3 helm chart repo and install the sheepdog service to your Kubernetes cluster. 

Deploying sheepdog this way will use the defaults that are defined in this [values.yaml file](https://github.com/uc-cdis/gen3-helm/blob/master/helm/sheepdog/values.yaml)
You can learn more about these values by accessing the sheepdog [README.md](https://github.com/uc-cdis/gen3-helm/blob/master/helm/sheepdog/README.md)

If you would like to override any of the default values, simply copy the above values.yaml file into a local file and make any changes needed. 

To deploy the service independant of other services (for testing purposes), you can set the .postgres.separate value to "true". This will deploy the service with its own instance of Postgres:
```bash
  postgres:
    separate: true
```

You can then supply your new values file with the following command: 
```bash
helm upgrade --install gen3/sheepdog -f values.yaml
```

If you are using Docker Build to create new images for testing, you can deploy them via Helm by replacing the .image.repository value with the name of your local image. 
You will also want to set the .image.pullPolicy to "never" so kubernetes will look locally for your image. 
Here is an example:
```bash
image:
  repository: <image name from docker image ls>
  pullPolicy: Never
  # Overrides the image tag whose default is the chart appVersion.
  tag: ""
```

Re-run the following command to update your helm deployment to use the new image: 
```bash
helm upgrade --install gen3/sheepdog
```

You can also store your images in a local registry. Kind and Minikube are popular for their local registries:
- https://kind.sigs.k8s.io/docs/user/local-registry/
- https://minikube.sigs.k8s.io/docs/handbook/registry/#enabling-insecure-registries

Dependencies:
Please review the "Quick Start with Helm" guides to deploy these two services.
- [Indexd](https://github.com/uc-cdis/indexd)
- [Fence](https://github.com/uc-cdis/fence)

## Installation

### Install Poetry

You can install Poetry.  Make sure the virtual environment is activated.

```console
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python
source $HOME/.poetry/env
```

You can install python dependencies using Poetry:

```console
poetry install -vv --no-interaction && poetry show -v
```

### Validate Installation

You can try to run the following to confirm installation:

```console
python3 run.py
```

For convenience, the minimal usage looks like the following:

```python
import datamodelutils
from dictionaryutils import dictionary
from gdcdictionary import gdcdictionary
from gdcdatamodel import models, validators
from flask import Flask
import sheepdog


dictionary.init(gdcdictionary)
datamodelutils.validators.init(validators)
datamodelutils.models.init(models)
blueprint = sheepdog.create_blueprint(name="submission")

app = Flask(__name__)
app.register_blueprint(blueprint)
```

You can also refer to the [sample script](../sample_usage.py) too.  If there's any issues with running the sample, revisit the installation.

> Note that `import sheepdog` relies on building and installing a local python egg for sheepdog, which can be accomplished using `python setup.py build` and `python setup.py install` in the local root directory for the clone repository.

### Generate Documentation

Auto-documentation is set up using [Sphinx](http://www.sphinx-doc.org/en/stable/). To build it, run

```console
cd docs
make html
```

which by default will output the index.html page to docs/build/html/index.html.

> Note that `make` should be available in the console.  For this guide, you can use `make` from WSL1.  You may also find that the *.rst files contain outdated definitions, so remove the definitions before building the documentation.

### Running tests

Before running the tests, make sure your virtual environment already activated.

```console
. py3-venv/bin/activate
```

For convenience, you can run the tests using [run_tests.bash](https://github.com/uc-cdis/sheepdog/blob/master/run_tests.bash).
> You may need to update the virtual environment activation step in the bash script before running it.  You can replace `source ~/virtualenv/python2.7/bin/activate` with `source py3-venv/bin/activate` then run the script with `bash run_tests.bash`.
You can use the following commands from the working directory for cloned repo in order to set up testing:

```console
python3 bin/setup_test_database.py
mkdir -p tests/integration/resources/keys; cd tests/integration/resources/keys; openssl genrsa -out test_private_key.pem 2048; openssl rsa -in test_private_key.pem -pubout -out test_public_key.pem; cd -
```

Then you can run the following for pytest:

```console
python3 -m pytest -vv --cov=sheepdog --cov-report xml tests/integration/datadict
python3 -m pytest -vv --cov=sheepdog --cov-report xml --cov-append tests/integration/datadictwithobjid
python3 -m pytest -vv --cov=sheepdog --cov-report xml --cov-append tests/unit
```