"""
Defines the sheepdog blueprint, which must be initialized with modules defining
the data dictionary and the data models.
"""

import flask

from sheepdog import sanity_checks


def create_blueprint(name):
    """
    Create the blueprint.

    Args:
        name: blueprint name

    Return:
        flask.Blueprint: the sheepdog blueprint
    """
    sanity_checks.validate()

    blueprint = flask.Blueprint(name, __name__)

    # Add all the routes defined in sheepdog.blueprint.routes to the new
    # blueprint.
    from sheepdog.blueprint.routes import routes

    for route in routes:
        blueprint.add_url_rule(
            route["rule"],
            endpoint=route["endpoint"],
            view_func=route["view_func"],
            **route["options"]
        )

    return blueprint
