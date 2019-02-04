import flask

from sheepdog import utils
from sheepdog.errors import UserError
from sheepdog.globals import FLAG_IS_ASYNC
from sheepdog.transactions.submission.transaction import SubmissionTransaction


def handle_submission_transaction(program, project, *doc_args, **tx_kwargs):
    """
    Create and execute a single (not bulk) transaction.

    Return:
        Tuple[flask.Response, int]: (API response json, status code)
    """
    is_async = tx_kwargs.pop("is_async", utils.is_flag_set(FLAG_IS_ASYNC))
    db_driver = tx_kwargs.pop("db_driver", flask.current_app.db)

    smtp_conf = None
    if utils.should_send_email(flask.current_app.config):
        smtp_conf = flask.current_app.get_smtp_conf()

    transaction = SubmissionTransaction(
        smtp_conf=smtp_conf,
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


def transaction_worker(transaction):
    """
    Perform a single transaction in the background after request context.

    Args:
        transaction: The transaction instance
    """
    session = transaction.db_driver.session_scope(can_inherit=False)
    with session, transaction:
        try:
            transaction.take_action()
        except UserError as e:
            transaction.record_user_error(e)
            raise
        except Exception as e:  # pylint: disable=broad-except
            transaction.record_internal_error(e)
        finally:
            response = transaction.json
            code = transaction.status_code
    return response, code
