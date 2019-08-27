"""
This module will depend on the downstream authutils dependency.

eg:
``pip install git+https://git@github.com/NCI-GDC/authutils.git@1.2.3#egg=authutils``
or
``pip install git+https://git@github.com/uc-cdis/authutils.git@1.2.3#egg=authutils``
"""

import functools

import flask


def authorize_for_project(*required_roles):
    """
    Wrap a function to allow access to the handler iff the user has at least
    one of the roles requested on the given project.
    """

    def wrapper(func):
        @functools.wraps(func)
        def authorize_and_call(program, project, *args, **kwargs):
            resource = "/programs/{}/projects/{}".format(program, project)
            flask.current_app.auth.auth_request({
                "requests": [
                    {
                        "resource": resource,
                        "action": {"service": "sheepdog", "method": role}
                    }
                    for role in required_roles
                ]
            })
            return func(program, project, *args, **kwargs)

        return authorize_and_call

    return wrapper
