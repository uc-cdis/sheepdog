# sheepdog

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/0069fa67707f48a7aabfe9de6b857392)](https://www.codacy.com/app/uc-cdis/sheepdog?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=uc-cdis/sheepdog&amp;utm_campaign=Badge_Grade)
[![Codacy Badge](https://api.codacy.com/project/badge/Coverage/0069fa67707f48a7aabfe9de6b857392)](https://www.codacy.com/app/uc-cdis/sheepdog?utm_source=github.com&utm_medium=referral&utm_content=uc-cdis/sheepdog&utm_campaign=Badge_Coverage)

## Installation

### For General Usage

To install sheepdog for use with other Gen3 services, running these commands is sufficient.

```bash
pip install -r requirements.txt
python setup.py build
python setup.py install
```

### For Development

```bash
pip install -r requirements.txt
pip install -r dev-requirements.txt
python setup.py develop
```

(`dev-requirements.txt` contains requirements for testing and doc generation.
Installing with `python setup.py develop` avoids literally installing anything
but creates an egg link to the source code.)

## Minimal Usage Example

```python
import sheepdog
from dictionaryutils import dictionary
from datamodels import models

# datadictionary = the data dictionary to use, e.g. gdcdictionary
# datamodels = the data model to use, e.g. gdcdatamodel

dictionary.init(datadictionary)
models.init(datamodels)
blueprint = sheepdog.create_blueprint()

app = Flask(__name__)
app.register_blueprint(blueprint)
```

## Documentation

Auto-documentation is set up using
[Sphinx](http://www.sphinx-doc.org/en/stable/). To build it, run
```bash
cd docs
make html
```
which by default will output the `index.html` page to
`docs/build/html/index.html`.
