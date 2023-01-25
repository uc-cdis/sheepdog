Requests Schema
===============

The schema for requests to the submission API is located at
`github.com/NCI_GDC/gdcdictionary
<https://github.com/NCI-GDC/gdcdictionary/>`_.

Specifying Entity Information
-----------------------------

When updating, creating, replacing, or deleting entities in the GDC system, you
need to specify the entity type, the entity id, any relationships the entity
has to parent entities from which it was derived, and any properties (required
and optional as defined by the entity schema).  The structure for each entity
should look as follows:

.. code-block:: javascript

    {
        'type': string,
        'id': string,
        'submitter_id': string,
        '<entity_property_keys>': any type,
        '<relationship_type>': [{
        'id': string,
        'submitter_id': string
        }],
        ...
    }

**id**
    *This or* ``submitter_id`` *required.* A string specifying the id of the ebject
    you are creating, updating, or deleting.  This is the official GDC ID for the
    entity.  If you prefer to refer to the entity using your custom id, you can
    do so with the ``submitter_id`` field.

**submitter_id**
    *This or* ``id`` *required.* A string specifying your custom id of the ebject
    you are creating, updating, or deleting.  This is not the official GDC ID for
    the entity.  If you prefer to refer to the entity using a GDC ID, you can do
    so with the ``@id`` field.

**<entity_property_keys>**
    All keys not listed above will be treated as properties keys.  These key
    value pairs will be used as properties on referenced entity.

**<relationship_type>**
    The type of a relationship.  The value for this is a JSON object specifying
    either the ``submitter_id`` or the ``id`` of the neighboring entity.

.. _label-response-format:

Response Format
---------------

The following fields should be included in all responses, regardless of
success.

.. code-block:: javascript

    {
        "code": int,
        "created_entity_count": 0,
        "entity": [object],
        "entity_error_count": string,
        "message": string,
        "success": boolean,
        "transactional_error_count": int,
        "transactional_errors": [transactional_error],
        "updated_entity_count": 0
    }

**success**
    A boolean value stating whether the transaction was successful.  If the value
    is `False`, then no changes will be made to the database.

**code**
    The HTTP status code of the response.

**message**
    A human readable summary of the transaction results.

**transactional_errors**
    A list of transactional errors occured.  These errors are errors that are
    not specific to an individual entity. Transactional errors are of the form

  .. code-block:: javascript

   {
     'message': string,
   }

**transactional_error_count**
    A count of the number of transactional errors that occured.

**entity_error_count**
    A count of the number entities that were not successful.

**entities**
    A list of entities of the form:

    .. code-block:: javascript

        {
            "submitter_id": string,
            "errors": [entity_errors],
            "id": string,
            "valid": boolean,
            "type": string,
        }

**entity_errors**

    A list of errors that occurred while parsing, validating, or
    performing a CRUD operation on a specific entity. Entity errors are
    of the form

    .. code-block:: javascript

        {
            'keys': [string],
            'message': string,
        }

For a listing of the types of errors, see :ref: `label-error-types`.

**created_entitiy_count**
    The number of entities created by the transaction.

**updated_entitiy_count**
    The number of existing entities updated by the transaction.

.. _label-creaing-entities:

Error Types
-----------

**EntityNotFoundError**
  A referenced entity was not found.  This includes both the
  transaction and the datamodel.

**MissingPropertyError**
 A required property was not provided.

**ValidationError**
  A provided property did not pass a validation test.


.. _label-status-messages:

Status Messages
---------------

API responses will contain a status for each entity specified in the request:

**success**
    The desired transaction was sucessful and the entity's state was
    modified in the database.  Because requests are transactional,
    either all entities will have status ``success`` or none will.

**valid**
    The desired transaction was not sucessful, but the trasaction was
    not aborted because of this entity.  Had all other entities in this
    transaction been ``valid`` and there were no internal errors, then
    the stats of this entity would ``success``.

**error**
    The desired transaction was not sucessful, and the trasaction was in
    part aborted because of this entity.  This entity did not pass
    validation or an internal error occured when attempting to complete
    the transaction. The ``error`` state will be accompanied by a list
    of errors recorded about the entity (see
    :ref: `label-error-messages`).
