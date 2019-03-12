# sheepdog

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/0069fa67707f48a7aabfe9de6b857392)](https://www.codacy.com/app/uc-cdis/sheepdog?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=uc-cdis/sheepdog&amp;utm_campaign=Badge_Grade)
[![Codacy Badge](https://api.codacy.com/project/badge/Coverage/0069fa67707f48a7aabfe9de6b857392)](https://www.codacy.com/app/uc-cdis/sheepdog?utm_source=github.com&utm_medium=referral&utm_content=uc-cdis/sheepdog&utm_campaign=Badge_Coverage)

## Installation

### For General Usage

To install sheepdog for use with other Gen3 services, running these commands is sufficient.

```bash
python setup.py build
python setup.py install
```

### For Development

```bash
pip install -r dev-requirements.txt
python setup.py develop
```

(`dev-requirements.txt` contains requirements for testing and doc generation.
Installing with `python setup.py develop` avoids literally installing anything
but creates an egg link to the source code.)

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

Auto-documentation is set up using
[Sphinx](http://www.sphinx-doc.org/en/stable/). To build it, run
```bash
cd docs
make html
```
which by default will output the `index.html` page to
`docs/build/html/index.html`.

### Swagger

[OpenAPI documentation available here.](http://petstore.swagger.io/?url=https://raw.githubusercontent.com/uc-cdis/sheepdog/master/openapi/swagger.yml)

The YAML file comtaining the OpenAPI documentation is in the `openapi` folder;
see the README in that folder for more details.

