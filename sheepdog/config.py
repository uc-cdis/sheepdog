import os
from cdislogging import get_logger

logger = get_logger(__name__)

# ======================================================================
# Legacy Mode

# LEGACY_MODE specifies whether the API will run in Legacy Mode. This
# mode will affect which mapping and index is used is used.


LEGACY_MODE = os.environ.get("GDC_API_LEGACY_MODE", "").lower() == "true"


if LEGACY_MODE:
    logger.info(
        "Running in LEGACY mode. The Elasticsearch 'GDC_ES_LEGACY_INDEX' "
        "environment variable and the legacy mapping will be used. "
    )
else:
    logger.info(
        "Running in ACTIVE mode. The Elasticsearch 'GDC_ES_INDEX' "
        "environment variable and the active mapping will be used. "
    )
