"""
Defines the sheepdog blueprint, which must be initialized with modules defining
the data dicitonary and the data models.
"""

import flask

from sheepdog import dictionary
from sheepdog import models


def create_blueprint(dictionary_to_use, models_to_use):
    """
    Create the blueprint.

    Args:
        dictionary_to_use: data dictionary such as gdcdictionary
        models_to_use: data model such as gdcdatamodel

    Return:
        flask.Blueprint: the sheepdog blueprint
    """
    dictionary.init(dictionary_to_use)
    models.init(models_to_use)

    blueprint = flask.Blueprint('submission', 'submission_v0')

    # Add all the routes defined in sheepdog.blueprint.routes to the new
    # blueprint.
    from sheepdog.blueprint.routes import routes
    for route in routes:
        blueprint.add_url_rule(
            route['rule'], endpoint=route['endpoint'],
            view_func=route['view_func'], **route['options']
        )

    return blueprint
