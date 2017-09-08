"""
Top-level functionality for closing dry_run transactions to prevent them from
being committed in the future.
"""

import flask
import sqlalchemy

from sheepdog import models
from sheepdog.errors import (
    NotFoundError,
    UserError,
)


def close_transaction(program, project, transaction_id):
    """
    Commit a dry_run transaction by repeating it with a new non-dry_run
    transaction.
    """
    with flask.current_app.db.session_scope():
        try:
            tx_log = (
                flask.current_app
                .db
                .nodes(models.submission.TransactionLog)
                .filter(models.submission.TransactionLog.id == transaction_id)
                .one()
            )
        except sqlalchemy.orm.exc.NoResultFound:
            raise NotFoundError(
                'Unable to find transaction_log with id: {} for project {}'
                .format(transaction_id, '{}-{}'.format(program, project)))

        # Check if already closed
        if tx_log.closed:
            raise UserError('This transaction log is already closed.')
        # Check if dry_run
        if tx_log.is_dry_run is False:
            raise UserError(
                'This transaction log is not a dry run. Closing it would have'
                ' no effect.'
            )
        # Check if dry_run
        if tx_log.committed_by is not None:
            raise UserError(
                'This transaction log has already been committed. Closing it'
                ' would have no effect.'
            )

        tx_log.closed = True

    return flask.jsonify({
        'code': 200,
        'message': 'Closed transaction.',
        'transaction_id': transaction_id,
    })
