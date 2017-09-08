### Installation

```bash
pip install -r requirements.txt
python setup.py build
python setup.py install
```

### Minimal Usage Example

```python
import sheepdog

# datadictionary = the data dictionary to use, e.g. gdcdictionary
# datamodels = the data model to use, e.g. gdcdatamodel
blueprint = sheepdog.create_blueprint(datadictionary, datamodels)

app = Flask(__name__)
app.register_blueprint(blueprint)
```

### Documentation

Auto-documentation is set up using
[Sphinx](http://www.sphinx-doc.org/en/stable/). To build it, run
```bash
cd docs
make html
```
which by default will output the `index.html` page to
`docs/_build/html/index.html`.
