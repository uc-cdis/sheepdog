import flask

from sheepdog import utils
from sheepdog.errors import UserError
from sheepdog.globals import FLAG_IS_ASYNC
from sheepdog.transactions.deletion.transaction import DeletionTransaction


def transaction_worker(transaction, ids):
    """
    Perform a single transaction in the background after request context.

    Args:
        transaction (DeletionTransaction): the transaction instance
    """
    session = transaction.db_driver.session_scope(can_inherit=False)

    with session, transaction:
        try:
            transaction.delete(ids)
        except UserError as exception:
            transaction.record_user_error(exception)
            raise
        except Exception as exception:  # pylint: disable=broad-except
            transaction.record_internal_error(exception)
        finally:
            response = transaction.json
            code = transaction.status_code

    return response, code


def handle_deletion_request(program, project, ids, to_delete=None, **tx_kwargs):
    """Create and execute a single deletion transaction.

    A user with administrator privileges can mark the sysan of
    the nodes as to_delete=True/False. The purpose of this is so
    esbuild will not use the nodes in creating Elastic Search indices.

    ex:
        /delete
        /to_delete/true
        /to_delete/false

    Args:
        program (string): program name
        project (string): project code
        ids (string): comma separated "list" of UUIDs to be deleted
        to_delete (bool): mark node with sysan['to_delete']=True/False
            if present
        tx_kwargs (dict): other transaction related variables

    Returns:
        flask.Response: API json response
        int: status code
    """

    is_async = tx_kwargs.pop('is_async', utils.is_flag_set(FLAG_IS_ASYNC))
    db_driver = tx_kwargs.pop('db_driver', flask.current_app.db)
    transaction = DeletionTransaction(
        program=program,
        project=project,
        logger=flask.current_app.logger,
        signpost=flask.current_app.signpost,
        to_delete=to_delete,
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

        flask.current_app.async_pool.schedule(
            transaction_worker, transaction, ids
        )
        return flask.jsonify(response), 200

    else:
        response, code = transaction_worker(transaction, ids)
        return flask.jsonify(response), code
