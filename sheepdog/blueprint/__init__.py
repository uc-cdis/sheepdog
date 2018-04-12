"""
Defines the sheepdog blueprint, which must be initialized with modules defining
the data dictionary and the data models.
"""

import flask

from sheepdog import sanity_checks
from sheepdog.blueprint.routes import routes


def create_blueprint(name, replace_views=None, ignore_routes=None):
    """
    Create the blueprint.

    Args:
        name: blueprint name
        ignore_routes: list of endpoints to ignore
        replace_views: dict {route: view_func}, used to overload endpoint behavior

    Return:
        flask.Blueprint: the sheepdog blueprint
    """
    if ignore_routes is None:
        ignore_routes = []

    if replace_views is None:
        replace_views = {}

    sanity_checks.validate()

    blueprint = flask.Blueprint(name, __name__)

    # Add routes defined in sheepdog.blueprint.routes to the new blueprint
    for route in routes:
        rule = route['rule']
        view = route['view_func']

        # Skip routes provided in ignore_routes
        if rule in ignore_routes:
            continue

        # Substitute routes' view functions if replace_views is set
        if rule in replace_views:
            view = replace_views[rule]

        # Add url_rule to the blueprint
        blueprint.add_url_rule(
            rule, endpoint=route['endpoint'],
            view_func=view, **route['options']
        )

    return blueprint
