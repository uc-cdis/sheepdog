"""
Defines the sheepdog blueprint, which must be initialized with modules defining
the data dictionary and the data models.
"""

import flask

from sheepdog import sanity_checks
from sheepdog.blueprint.routes import routes
from cdiserrors import UserError


def assert_route_is_valid(route):
    """
    Checks that route dictionary has all nessesary keys provided
    """
    def check_key(dictionary, key):
        if key not in dictionary:
            raise UserError('Required key "{}" is not provided. Can not continue.'
                            .format(key))

    check_key(route, 'rule')
    check_key(route, 'endpoint')
    check_key(route, 'view_func')
    check_key(route, 'options')
    check_key(route['options'], 'methods')


def create_blueprint(name, replace_views=None, ignore_routes=None):
    """
    Create the blueprint.

    Args:
        name: blueprint name
        ignore_routes: list of endpoints to ignore
        replace_views: dict {url_rule: route}, used to overload endpoint behavior

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

        # Skip routes provided in ignore_routes
        if rule in ignore_routes:
            continue

        # Substitute routes' view functions if replace_views is set
        if rule in replace_views:
            route = replace_views[rule]

        assert_route_is_valid(route)

        # Add url_rule to the blueprint
        blueprint.add_url_rule(
            route['rule'], endpoint=route['endpoint'],
            view_func=route['view_func'], **route['options']
        )

    return blueprint
