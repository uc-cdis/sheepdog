"""
This module checks that both loaded dictionary and models
have required attributes for sheepdog. It's called by
sheepdog.blueprint.create_blueprint to make sure that datamodelutils
and dictionaryutils is correctly initialized.
example:

.. code-block:: python
    sheepdog.sanity_checks.validate()
"""

from sheepdog import dictionary, models, validators

#: The data dictionary must implement these attributes.
DICTIONARY_REQUIRED_ATTRS = [
    'resolvers',
    'schema',
]

MODELS_REQUIRED_ATTRS = [
    'Program',
    'Project',
    'submission',
    'VersionedNode',
]

VALIDATORS_REQUIRED_ATTRS = [
    'GDCGraphValidator',
    'GDCJSONValidator',
]

def validate():
    """
    Check that both loaded dictionary and models have
    required attributes for sheepdog
    """
    check_attributes(dictionary, DICTIONARY_REQUIRED_ATTRS)
    check_attributes(models, MODELS_REQUIRED_ATTRS)
    check_attributes(validators, VALIDATORS_REQUIRED_ATTRS)


def check_attributes(module, required_attrs):
    """
    Check if a module have a list of required attributes
    module: target module
    required_attrs (str[]): a list of required attributes

    Return:
        None
    """
    for required_attr in required_attrs:
        if not hasattr(module, required_attr):
            raise ValueError(
                'given dictionary does not define ' + required_attr)
