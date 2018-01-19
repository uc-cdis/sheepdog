# pylint: disable=no-member
"""
Pylint ``no-member`` error disabled because of false positives with
``lxml.etree``.
"""

import uuid

import flask
import lxml

from sheepdog import auth
from sheepdog import utils
from sheepdog.errors import (
    ParsingError,
    SchemaError,
    UnsupportedError,
    UserError,
)
from sheepdog.globals import (
    FLAG_IS_ASYNC,
    PROJECT_SEED,
)
from sheepdog.transactions.upload.entity import UploadEntity
from sheepdog.transactions.upload.transaction import (
    BulkUploadTransaction,
    UploadTransaction,
)


def single_transaction_worker(transaction, *doc_args):
    """
    Execute single transaction (called in serial or async).
    """
    session = transaction.db_driver.session_scope(can_inherit=False)
    with session, transaction:
        try:
            transaction.parse_doc(*doc_args)
            transaction.flush()
            transaction.post_validate()
            transaction.commit()
        except UserError as e:
            transaction.record_user_error(e)
            raise
        except Exception as e:  # pylint: disable=broad-except
            transaction.record_internal_error(e)
        finally:
            response = transaction.json
            code = transaction.status_code
    return response, code


def _single_transaction(role, program, project, *doc_args, **tx_kwargs):
    """
    Create and execute a single (not bulk) transaction.

    Most non-bulk variations of upload actions (based on content-type,
    etc.) should wrap this function.

    Return:
        Tuple[flask.Response, int]: (API response json, status code)
    """
    is_async = tx_kwargs.pop('is_async', utils.is_flag_set(FLAG_IS_ASYNC))
    db_driver = tx_kwargs.pop('db_driver', flask.current_app.db)

    transaction = UploadTransaction(
        program=program,
        project=project,
        role=role,
        user=flask.g.user,
        logger=flask.current_app.logger,
        signpost=flask.current_app.signpost,
        db_driver=db_driver,
        external_proxies=utils.get_external_proxies(),
        **tx_kwargs
    )

    if is_async:
        session = transaction.db_driver.session_scope(can_inherit=False)
        with session, transaction:
            response = {
                "code": 200,
                "message": "Transaction submitted.",
                "transaction_id": transaction.transaction_id,
            }
        flask.current_app.async_pool.schedule(
            single_transaction_worker, transaction, *doc_args
        )
        return flask.jsonify(response)
    else:
        response, code = single_transaction_worker(transaction, *doc_args)
        return flask.jsonify(response), code


def handle_single_transaction(role, program, project, **tx_kwargs):
    """
    Main entry point for single file transactions.

    This function multiplexes on the content-type to call the appropriate
    transaction handler.
    """
    doc = flask.request.get_data()
    content_type = flask.request.headers.get('Content-Type', '').lower()
    if content_type == 'text/csv':
        doc_format = 'csv'
        data, errors = utils.transforms.CSVToJSONConverter().convert(doc)
    elif content_type in ['text/tab-separated-values', 'text/tsv']:
        doc_format = 'tsv'
        data, errors = utils.transforms.TSVToJSONConverter().convert(doc)
    else:
        doc_format = 'json'
        data = utils.parse.parse_request_json()
        errors = None
    # TODO: use errors value?
    name = flask.request.headers.get('X-Document-Name', None)
    doc_args = [name, doc_format, doc, data]
    is_async = tx_kwargs.pop('is_async', utils.is_flag_set(FLAG_IS_ASYNC))
    db_driver = tx_kwargs.pop('db_driver', flask.current_app.db)
    transaction = UploadTransaction(
        program=program, project=project, role=role, user=flask.g.user,
        logger=flask.current_app.logger, signpost=flask.current_app.signpost,
        external_proxies=utils.get_external_proxies(),
        db_driver=db_driver, **tx_kwargs
    )
    if is_async:
        session = transaction.db_driver.session_scope(can_inherit=False)
        with session, transaction:
            response = {
                "code": 200,
                "message": "Transaction submitted.",
                "transaction_id": transaction.transaction_id,
            }
        flask.current_app.async_pool.schedule(
            single_transaction_worker, transaction, *doc_args
        )
        return flask.jsonify(response)
    else:
        response, code = single_transaction_worker(transaction, *doc_args)
        return flask.jsonify(response), code


def unpack_bulk_wrapper(wrapper):
    """Return the name, the doc, and the doc_format from the wrapper."""
    return (
        wrapper.get('name'),
        wrapper.get('doc', ''),
        wrapper.get('doc_format'),
    )


def _add_wrapper_to_bulk_transaction(transaction, wrapper, index):
    required_keys = {'doc_format', 'doc', 'name'}
    # Check object keys
    if required_keys - set(wrapper.keys()):
        raise UserError(
            'Missing required field in document {}: {}'
            .format(index, list(required_keys - set(wrapper.keys())))
        )

    name, doc, doc_format = unpack_bulk_wrapper(wrapper)

    # Parse doc
    doc_format = wrapper['doc_format'].lower()
    if doc_format == 'json':
        try:
            data = utils.parse.parse_json(doc)
        except Exception as e:
            raise UserError('Unable to parse doc {}: {}'.format(name, e))
    elif doc_format == 'tsv':
        data, errors = utils.transforms.TSVToJSONConverter().convert(doc)
    elif doc_format == 'csv':
        data, errors = utils.transforms.CSVToJSONConverter().convert(doc)
    else:
        raise UnsupportedError(doc_format)

    # Add doc to transaction
    transaction.add_doc(name, doc_format, doc, data)


def bulk_transaction_worker(transaction, wrappers):
    session = transaction.db_driver.session_scope(can_inherit=False)

    with session, transaction:
        # Add all docs to bulk transaction
        for i, wrapper in enumerate(wrappers):
            try:
                _add_wrapper_to_bulk_transaction(transaction, wrapper, i)
            except UserError as exception:
                name, doc, doc_format = unpack_bulk_wrapper(wrapper)
                transaction.add_doc(name, doc_format, doc, {})
                transaction.record_user_error(exception)
                raise
            except Exception as exception:  # pylint: disable=broad-except
                name, doc, doc_format = unpack_bulk_wrapper(wrapper)
                transaction.add_doc(name, doc_format, doc, {})
                transaction.record_internal_error(exception)

        try:
            # Validate everything together.  This flush will make
            # sure that all valid entities from all
            # subtransactions have nodes bound to the current
            # session.  This allows entities to be added out of
            # order in the loop above by delaying post_validate
            # checks until after all nodes are accounted for.
            transaction.flush()
            transaction.post_validate()
            transaction.commit()
        except UserError as e:
            transaction.record_user_error(e)
            raise
        except Exception as e:  # pylint: disable=broad-except
            transaction.record_internal_error(e)
        finally:
            response = transaction.json

        return response, response['code']


def handle_bulk_transaction(role, program, project, **tx_kwargs):
    """
    TODO
    """
    wrappers = utils.parse.parse_request_json()
    # Assert wrapper is list of JSON objects
    invalid_format_msg = (
        'Bulk transfers must be an array of JSON objects of format: {\n'
        '    "name": string,\n'
        '    "doc_format": string,\n'
        '    "doc": string,\n'
        '}'
    )
    if not isinstance(wrappers, list):
        raise UserError(invalid_format_msg)

    for wrapper in wrappers:
        if not isinstance(wrapper, dict):
            raise UserError(invalid_format_msg)

    is_async = tx_kwargs.pop('is_async', utils.is_flag_set(FLAG_IS_ASYNC))

    transaction = BulkUploadTransaction(
        program=program,
        project=project,
        role=role,
        user=flask.g.user,
        logger=flask.current_app.logger,
        signpost=flask.current_app.signpost,
        db_driver=flask.current_app.db,
        external_proxies=utils.get_external_proxies(),
        **tx_kwargs
    )

    if is_async:
        session = transaction.db_driver.session_scope(can_inherit=False)
        with session, transaction:
            response = {
                "code": 200,
                "message": "Transaction submitted.",
                "transaction_id": transaction.transaction_id,
            }
        flask.current_app.async_pool.schedule(
            bulk_transaction_worker, transaction, wrappers
        )
        return flask.jsonify(response)
    else:
        response, code = bulk_transaction_worker(transaction, wrappers)
        return flask.jsonify(response), code


def handle_biospecimen_bcr_xml_transaction(
        role, program, project, **tx_kwargs):
    """
    Entrypoint from the flask blueprint for BCR Biospecimen XML XSD 2.6
    """
    project_node_id = str(uuid.uuid5(PROJECT_SEED, project.encode('utf-8')))
    parser = utils.transforms.BcrXmlToJsonParser(project_node_id)
    return handle_xml_transaction(role, program, project, parser, **tx_kwargs)


def handle_clinical_bcr_xml_transaction(role, program, project, **tx_kwargs):
    """
    Entrypoint from the flask blueprint for BCR Clinical XML XSD 2.6
    """
    parser = utils.transforms.BcrClinicalXmlToJsonParser(project)
    return handle_xml_transaction(role, program, project, parser, **tx_kwargs)



def handle_xml_transaction(role, program, project, parser, **tx_kwargs):
    """
    Handle XML transactions. Provide a parser that has a function with the
    signature ``parser.loads(doc)`` to load a doc and ``parser.json`` property
    to retreive the parsed docs.
    """
    parsing_errors = (
        lxml.etree.XMLSchemaError,
        lxml.etree.XMLSyntaxError,
        lxml.etree.DocumentInvalid,
        SchemaError,
    )
    try:
        parser.loads(flask.request.get_data())
    except parsing_errors as e:  # pylint: disable=catching-non-exception
        flask.current_app.logger.error(e)
        raise UserError('Unable to parse xml: {}'.format(e))

    except Exception as exc:
        flask.current_app.logger.exception(exc)
        raise UserError('Unable to parse xml')

    data = parser.json
    original = flask.request.get_data()
    name = flask.request.headers.get('X-Document-Name', None)

    return _single_transaction(
        role, program, project, name, 'XML', original, data, **tx_kwargs)
