"""
Notes on frequently disabled pylint warnings/errors:

* ``unsubscriptable-object``: because ``sheepdog.dictionary`` must be
  initialized later by the application using sheepdog, it appears to
  pylint that operations such as ``dictionary.schema[entity_type]`` should not
  work.
* ``unsupported-membership-test``: again because of
  ``sheepdog.dictionary``.
"""

from dictionaryutils import dictionary
from datamodelutils import models, validators
from .blueprint import create_blueprint

from cdislogging import get_logger

# Can't read config yet. Just set to debug for now.
# Later, in app.app_init(), will actually set level based on config
logger = get_logger("audit-service", log_level="debug")
