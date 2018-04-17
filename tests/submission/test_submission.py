from gdcdatamodel import models as md
from gdcdatamodel.models.submission import (
    TransactionDocument,
    TransactionLog,
    TransactionSnapshot,
)
from sheepdog.globals import REQUEST_SUBMIT_KEY
import pytest
from envelopes import SMTP

from test_endpoints import (
    put_example_entities_together,
)

from utils import put_entity_from_file


BASE = '/v0/submission/CGCI/BLGSP'


@pytest.fixture
def patch_envelopes(smtpserver, monkeypatch):
    """Patch envelopes.connstack to return connections pointing to the
    mock smtpserver

    """

    def mock_smtp(*args, **kwargs):
        """Monkeypatch SMTP to return a connection to test server"""

        host, port = smtpserver.addr

        # TODO: mock with tls and auth
        return SMTP(host=host, port=port)

    monkeypatch.setattr('envelopes.SMTP', mock_smtp)

    return smtpserver


def reset_transactions(pg_driver):
    with pg_driver.session_scope() as s:
        s.query(TransactionSnapshot).delete()
        s.query(TransactionDocument).delete()
        s.query(TransactionLog).delete()


def setup_database(client, pg_driver, submitter, reset_tx_logs=True):
    r = put_example_entities_together(client, submitter)

    if reset_tx_logs:
        reset_transactions(pg_driver)

    assert r.status_code == 200, r.data


def test_submit_invalid_state(client, pg_driver, cgci_blgsp, patch_envelopes, submitter):
    """Test that submission only works in state ``review``"""

    setup_database(client, pg_driver, submitter)
    r = client.put(BASE+'/submit', headers=submitter)
    assert r.status_code == 400, 'This should have failed!'


def test_submit_simple(client, index_client, pg_driver, cgci_blgsp, patch_envelopes, submitter):
    """
    Test a simple user submission.
    - Node states must NOT change from 'validated' to 'submitted' (this happens on admin submission)
    - Project node's property {REQUEST_SUBMIT_KEY} supposed to be set to True
    - One email must be send
    """

    setup_database(client, pg_driver, submitter)
    put_entity_from_file(client, 'read_group.json', submitter)
    put_entity_from_file(client, 'submitted_unaligned_reads.json', submitter)

    # change project to state reviewed
    client.put(BASE+'/review', headers=submitter)

    # Submit the project
    r = client.put(BASE+'/submit', headers=submitter)
    assert r.status_code == 200, r.data

    # We want to make sure all entities in the project have REQUEST_SUBMIT_KEY == True
    # but still have 'validated' state
    with pg_driver.session_scope():
        nodes = pg_driver.nodes().props(project_id='CGCI-BLGSP').all()
        for node in nodes:
            # Check that node states did not change
            assert node.state == 'validated', node

        project_node = pg_driver.nodes(md.Project).props(code='BLGSP').first()
        assert project_node.releasable is True
        # Check that project is requested for submission
        assert getattr(project_node, REQUEST_SUBMIT_KEY) is True

    # Check that email was sent
    assert len(patch_envelopes.outbox) == 1


def test_transaction_log(client, pg_driver, cgci_blgsp, patch_envelopes, submitter):
    """Verify submission creates a valid transaction_log"""

    setup_database(client, pg_driver, submitter)
    client.put(BASE+'/review', headers=submitter)
    client.put(BASE+'/submit', headers=submitter)

    # We want to make sure there is a transaction log and that all
    # entities have action ``submit``
    with pg_driver.session_scope() as s:
        logs = (s.query(TransactionLog).
                order_by(TransactionLog.id)
                .all())
        assert len(logs) == 2
        assert (logs[1].documents[0].response_json['entities'][0]['action']
                == 'submit')
