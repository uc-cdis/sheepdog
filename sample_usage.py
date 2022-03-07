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
