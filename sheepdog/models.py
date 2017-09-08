"""
This module generalizes the data model used by the sheepdog blueprint, and
must be initialized using another ``models`` module to set the attributes of
this module. For example, using ``gdcdatamodel.models`` as the models:

.. code-block:: python

    sheepdog.models.init(gdcdatamodel.models)

Then this module can be imported elsewhere in ``sheepdog``:

.. code-block:: python

    from sheepdog import models

    # This is effectively an alias of ``gdcdatamodel.models.Project``.
    models.Project
"""

import sys


# Get this module as a variable so its attributes can be set later.
this_module = sys.modules[__name__]

#: The data model must implement these attributes.
required_attrs = [
    'Program',
    'Project',
    'submission',
    'VersionedNode',
]

# These could be assigned programatically, as in:
#
#     for required_attr in required_attrs:
#         setattr(this_module, required_attr, None)
#
# but setting them individually prevents errors from pylint etc.

Program = None
Project = None
submission = None
VersionedNode = None


def init(models):
    """
    Initialize this file with the same attributes as ``models`` to be imported
    elsewhere in ``sheepdog``.

    Args:
        models: a module that should implement the required attributes above

    Return:
        None
    """
    for required_attr in required_attrs:
        try:
            # Basically do: this_module.required_attr = models.required_attr
            setattr(this_module, required_attr, getattr(models, required_attr))
        except AttributeError:
            raise ValueError('given models does not define ' + required_attr)
