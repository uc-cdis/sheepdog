# sheepdog

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/0069fa67707f48a7aabfe9de6b857392)](https://www.codacy.com/app/uc-cdis/sheepdog?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=uc-cdis/sheepdog&amp;utm_campaign=Badge_Grade)
[![Codacy Badge](https://api.codacy.com/project/badge/Coverage/0069fa67707f48a7aabfe9de6b857392)](https://www.codacy.com/app/uc-cdis/sheepdog?utm_source=github.com&utm_medium=referral&utm_content=uc-cdis/sheepdog&utm_campaign=Badge_Coverage)

## Installation

### For General Usage

To install sheepdog, ensure you have poetry installed and run:

```bash
poetry install
```

## Minimal Usage Example

```python
import sheepdog
import datamodelutils
from dictionaryutils import dictionary
from gdcdictionary import gdcdictionary
from gdcdatamodel import models, validators

dictionary.init(gdcdictionary)
datamodelutils.validators.init(validators)
datamodelutils.models.init(models)
blueprint = sheepdog.create_blueprint(name='submission')

app = Flask(__name__)
app.register_blueprint(blueprint)
```

## Documentation

### Sphinx

Auto-documentation is set up using [Sphinx](http://www.sphinx-doc.org/en/stable/). To build it, run
```bash
cd docs
make html
```
which by default will output the `index.html` page to
`docs/build/html/index.html`.

### Swagger

[OpenAPI documentation available here.](http://petstore.swagger.io/?url=https://raw.githubusercontent.com/uc-cdis/sheepdog/master/openapi/swagger.yml)

The YAML file containing the OpenAPI documentation is in the `openapi` folder;
see the README in that folder for more details.

## Gen3 graph data flow

<img src="docs/Gen3 graph data flow.png" width="70%">

## Submitter ID
Sheepdog requires the `submitter_id` to be unique per node per project. It means that, the `submitter_id` of all `case` nodes must be unique per project. This constraint was technically enforced by the unique index of `(project_id, submitter_id)` in every node table.

## Local Test Run Using CI Scripts

If you want to locally replicate what GH Actions is doing more closely, follow
these steps. 

Ensure you've run `poetry install`. 

Ensure you have Postgresql 13 set up and running.

Ensure there is a postgres user `postgres` *and* `test` setup with password `test`:

```
CREATE USER postgres WITH PASSWORD 'test';
```

Then run:

```bash
bash tests/ci_setup.sh
```

If the above fails due to postgres errors, your postgresql setup may need some 
fixing, it should finish with the following:

```
Setting up test database
Dropping old test data
WARNING:root:Unable to drop test data:(psycopg2.errors.InvalidCatalogName) database "sheepdog_automated_test" does not exist

[SQL: DROP DATABASE "sheepdog_automated_test"]
(Background on this error at: http://sqlalche.me/e/13/f405)
Creating tables in test database
Creating indexes
writing RSA key
```

The WARNING will show up the first time you run this, it's safe to ignore.

That sets up the database so if you run into postgres errors, you'll want to 
double check your postgres setup.

After that you can run unit tests with:

```bash
bash tests/ci_commands_script.sh
```

> You can see more detailed information on local dev setup in the docs/local_dev_environment.md