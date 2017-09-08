"""
Defines transactions to transition the project to and from ``review``.

TODO(jsm|2016-02-01): Add review constraints.
"""

import flask

from sheepdog import utils
from sheepdog.globals import (
    FLAG_IS_ASYNC,
)
from sheepdog.transactions.review.transaction import (
    OpenTransaction,
    ReviewTransaction,
)


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


def _single_transaction(tx_cls, program, project, **tx_kwargs):
    """
    Create and execute a single (not bulk) transaction.

    Args:
        tx_cls: The transaction class

    Return:
        Tuple[flask.Response, int]: (API response json, status code)
    """
    is_async = tx_kwargs.pop('is_async', utils.is_flag_set(FLAG_IS_ASYNC))
    db_driver = tx_kwargs.pop('db_driver', flask.current_app.db)

    transaction = tx_cls(
        program=program,
        project=project,
        user=flask.g.user,
        logger=flask.current_app.logger,
        signpost=flask.current_app.signpost,
        db_driver=db_driver,
        **tx_kwargs
    )

    if is_async:
        session = transaction.db_driver.session_scope()
        with session, transaction:
            response = {
                'code': 200,
                'message': 'Transaction submitted.',
                'transaction_id': transaction.transaction_id,
            }
        flask.current_app.async_pool.schedule(transaction_worker, transaction)
        return flask.jsonify(response), 200
    else:
        response, code = transaction_worker(transaction)
        return flask.jsonify(response), code


def handle_review_transaction(program, project, **tx_kwargs):
    """Attempt to take review action."""
    return _single_transaction(
        ReviewTransaction,
        program,
        project,
        **tx_kwargs
    )


def handle_open_transaction(program, project, **tx_kwargs):
    """Attempt to take review action."""
    return _single_transaction(
        OpenTransaction,
        program,
        project,
        **tx_kwargs
    )
