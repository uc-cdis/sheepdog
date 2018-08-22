"""
Defines transactions to release the project.

TODO(jsm|2016-02-01): Add review constraints.
"""

import flask

from sheepdog import utils
from sheepdog.globals import (
    FLAG_IS_ASYNC,
)
from sheepdog.transactions.transaction_base import TransactionBase
from sheepdog.transactions.release.transaction import ReleaseTransaction


def transaction_worker(transaction):
    """
    Perform a single transaction in the background after request context.

    Args:
        transaction: The transaction instance
    """
    session = transaction.db_driver.session_scope(can_inherit=False)
    with session, transaction:
        transaction.take_action()
        return transaction.json, transaction.status_code


def handle_release_transaction(program, project, **tx_kwargs):
    """
    Create and execute a single transaction.

    Return:
        Tuple[flask.Response, int]: (API response json, status code)
    """
    is_async = tx_kwargs.pop('is_async', utils.is_flag_set(FLAG_IS_ASYNC))
    db_driver = tx_kwargs.pop('db_driver', flask.current_app.db)

    transaction = ReleaseTransaction(
        program=program,
        project=project,
        logger=flask.current_app.logger,
        signpost=flask.current_app.signpost,
        db_driver=db_driver,
        **tx_kwargs
    )

    if is_async:
        session = transaction.db_driver.session_scope()
        with session, transaction:
            response = {
                "code": 200,
                "message": "Transaction submitted.",
                "transaction_id": transaction.transaction_id,
            }
        flask.current_app.async_pool.schedule(transaction_worker, transaction)
        return flask.jsonify(response), 200
    else:
        response, code = transaction_worker(transaction)
        return flask.jsonify(response), code
