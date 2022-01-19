"""
Copy of a few functional tests in order to test that SSL and isolation level
settings work.
"""

import csv
import json
import pytest
import os

from .submission.test_endpoints import (
    BLGSP_PATH,
    DATA_DIR,
    post_example_entities_together,
    do_test_export,
)


USE_SSL = [False, True, None]
ISOLATION_LEVELS = ["READ_COMMITTED", "REPEATABLE_READ", "SERIALIZABLE", None]


@pytest.mark.parametrize("use_ssl", USE_SSL, indirect=True)
@pytest.mark.parametrize("isolation_level", ISOLATION_LEVELS, indirect=True)
def test_post_example_entities_together(client, pg_driver, cgci_blgsp, submitter):
    with open(os.path.join(DATA_DIR, "case.json"), "r") as f:
        case_sid = json.loads(f.read())["submitter_id"]
        print(case_sid)
    resp = post_example_entities_together(client, submitter)
    resp_data = json.loads(resp.data)
    condition_to_check = (resp.status_code == 201 and resp.data) or (
        resp.status_code == 400
        and "already exists in the DB"
        in resp_data["entities"][0]["errors"][0]["message"]
    )
    assert condition_to_check, resp.data


@pytest.mark.parametrize("use_ssl", USE_SSL, indirect=True)
@pytest.mark.parametrize("isolation_level", ISOLATION_LEVELS, indirect=True)
def test_delete_entity(client, pg_driver, cgci_blgsp, submitter):
    resp = client.put(
        BLGSP_PATH,
        headers=submitter,
        data=json.dumps(
            {
                "type": "experiment",
                "submitter_id": "BLGSP-71-06-00019",
                "projects": {"id": "daa208a7-f57a-562c-a04a-7a7c77542c98"},
            }
        ),
    )
    assert resp.status_code == 200, resp.data
    did = resp.json["entities"][0]["id"]
    path = BLGSP_PATH + "entities/" + did
    resp = client.delete(path, headers=submitter)
    assert resp.status_code == 200, resp.data


@pytest.mark.parametrize("use_ssl", USE_SSL, indirect=True)
@pytest.mark.parametrize("isolation_level", ISOLATION_LEVELS, indirect=True)
def test_submit_valid_tsv(client, pg_driver, cgci_blgsp, submitter):
    """
    Test that we can submit a valid TSV file
    """

    data = {
        "type": "experiment",
        "submitter_id": "BLGSP-71-06-00019",
        "projects.id": "daa208a7-f57a-562c-a04a-7a7c77542c98",
    }

    # convert to TSV (save to file)
    file_path = os.path.join(DATA_DIR, "experiment_tmp.tsv")
    with open(file_path, "w") as f:
        dw = csv.DictWriter(f, sorted(data.keys()), delimiter="\t")
        dw.writeheader()
        dw.writerow(data)

    # read the TSV data
    data = None
    with open(file_path, "r") as f:
        data = f.read()
    os.remove(file_path)  # clean up (delete file)
    assert data

    headers = submitter
    headers["Content-Type"] = "text/tsv"
    resp = client.put(BLGSP_PATH, headers=headers, data=data)
    assert resp.status_code == 200, resp.data


@pytest.mark.parametrize("use_ssl", USE_SSL, indirect=True)
@pytest.mark.parametrize("isolation_level", ISOLATION_LEVELS, indirect=True)
def test_export_all_node_types(
    client, pg_driver, cgci_blgsp, submitter, require_index_exists_off
):
    do_test_export(client, pg_driver, submitter, "experimental_metadata", "tsv")
