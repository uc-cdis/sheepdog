import sheepdog
import datamodelutils
from flask import Flask
from dictionaryutils import dictionary
from gdcdictionary import gdcdictionary
from gdcdatamodel import models, validators

dictionary.init(gdcdictionary)
datamodelutils.validators.init(validators)
datamodelutils.models.init(models)
blueprint = sheepdog.create_blueprint(name="submission")

app = Flask(__name__)
app.register_blueprint(blueprint)
