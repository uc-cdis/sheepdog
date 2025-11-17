"""
Copy of a few functional tests in order to test that isolation level
settings work.
"""

import pytest

from tests.integration.datadictwithobjid.submission.test_endpoints import (
    do_test_post_example_entities_together,
)
from tests.integration.datadict.submission.test_endpoints import (
    do_test_delete_entity,
    do_test_submit_valid_tsv,
    do_test_export,
)


USE_SSL = [False, True, None]
ISOLATION_LEVELS = ["READ_COMMITTED", "REPEATABLE_READ", "SERIALIZABLE", None]


@pytest.mark.ssl
@pytest.mark.parametrize("isolation_level", ISOLATION_LEVELS, indirect=True)
def test_post_example_entities_together(client, pg_driver, cgci_blgsp, submitter):
    do_test_post_example_entities_together(client, submitter)


@pytest.mark.ssl
@pytest.mark.parametrize("isolation_level", ISOLATION_LEVELS, indirect=True)
def test_delete_entity(client, pg_driver, cgci_blgsp, submitter):
    do_test_delete_entity(client, submitter)


@pytest.mark.ssl
@pytest.mark.parametrize("isolation_level", ISOLATION_LEVELS, indirect=True)
def test_submit_valid_tsv(client, pg_driver, cgci_blgsp, submitter):
    do_test_submit_valid_tsv(client, submitter)


@pytest.mark.ssl
@pytest.mark.parametrize("isolation_level", ISOLATION_LEVELS, indirect=True)
def test_export_all_node_types(
    client, pg_driver, cgci_blgsp, submitter, require_index_exists_off
):
    do_test_export(
        client,
        pg_driver,
        submitter,
        "experimental_metadata",
        "tsv",
        test_add_new_experimental_metadata=True,
    )
