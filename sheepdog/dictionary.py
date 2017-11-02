"""
This module generalizes the data dictionary used by the sheepdog
blueprint, and must be initialized using another ``dictionary`` module to set
the attributes of this module. For example, using
``gdcdictionary.gdcdictionary`` as the dictionary:

.. code-block:: python

    sheepdog.dictionary.init(gdcdictionary.gdcdictionary)
"""

import sys


# Get this module as a variable so its attributes can be set later.
this_module = sys.modules[__name__]

#: The data dictionary must implement these attributes.
required_attrs = [
    'resolvers',
    'schema',
]

optional_attrs = [
    'settings',
]

resolvers = None
schema = None
settings = None

def init(dictionary):
    """
    Initialize this file with the same attributes as ``dictionary`` to be
    imported elsewhere in ``sheepdog``.

    Args:
        dictionary:
            a module that should implement the required attributes above, and
            optionally the optional attributes

    Return:
        None
    """
    for required_attr in required_attrs:
        try:
            # Basically do: this_module.required_attr = models.required_attr
            setattr(
                this_module, required_attr, getattr(dictionary, required_attr)
            )
        except AttributeError:
            raise ValueError('given dictionary does not define ' + required_attr)

    for optional_attr in optional_attrs:
        try:
            # Basically do: this_module.required_attr = models.required_attr
            setattr(
                this_module, optional_attr, getattr(dictionary, optional_attr)
            )
        except AttributeError:
            pass
