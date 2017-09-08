Entity Usage
============

Creating Entities
-----------------

**Example Usage for Creating Entities**

The following example will:

#. Create a new aliquot ``aliquot-1``.
#. Specify that ``aliquot-1`` was derived from analyte ``analyte-1``.
#. Specify that ``analyte-1`` was derived from existing portion ``portion-1``.

.. code-block:: http

    POST /v0/submission/program1/project1/ HTTP/1.1
    Host: example.com
    Content-Type: application/json
    X-Auth-Token: MIIDKgYJKoZIhvcNAQcC...
    Accept: application/json

.. code-block:: JavaScript

    [{
            "type": "analyte",
            "portions": {
                "submitter_id": "portion-1"
            },
            "analyte_type": "DNA",
            "submitter_id": "analyte-1",
            "amount": 10.98,
            "concentration": 0.14,
            "spectrophotometer_method": "PicoGreen",
            "analyte_type_id": "D"
        }, {
            "type": "aliquot",
            "analytes": {
                "submitter_id": "analyte-1"
            },
            "submitter_id": "aliquot-1",
            "source_center": "23",
            "concentration": 0.07
        }]

**Example successful result:**

.. code-block:: http

    HTTP/1.1 201 CREATED
    Content-Type: application/json

.. code-block:: JavaScript

    {
        "code": 201,
        "created_entity_count": 1,
        "entities": [
        {
            "submitter_id": "analyte-1",
            "errors": [],
            "id": "2e1429d5-b2ec-4c02-93ac-207d10b1193c",
            "valid": true,
            "type": "analyte"
        },
        {
            "submitter_id": "aliquot-1",
            "errors": [],
            "id": "6a30b20a-1e38-4c16-8c16-c03ab30f7a11",
            "valid": true,
            "type": "aliquot"
        }
        ],
        "entity_error_count": 0,
        "message": "Transaction successful.",
        "success": true,
        "transactional_error_count": 0,
        "transactional_errors": [],
        "updated_entity_count": 0
    }

**Successful example response:**

In the successful example response, the analyte will be created,
assigned ID ``2e1429d5...``, and linked to portion
``portion-1``. The aliquot will be created, assigned an ID, and
linked to analyte ``analyte-1``.  Note that the portion
``portion-1`` referenced by ``analyte-1`` above is not included
in the transaction.  Part of the validation for the creation of
any entity is to check if:

1. The entity it was derived from exists in the current transaction, If
the parent entity was in the transaction, verify that any
information provided does not conflict with the existing version
2. If the parent entity was not in the transaction, verify that it
already exists in the system

Just as the portion referenced by ``portion-1`` was uploaded in a
previous transaction, we could have split this example into two
transactions, the first creating the aliquot, and the second
creating the file.

.. note::
    The GDC will not allow entities to exist without knowing what they
    were derived from. For example, you cannot upload an aliquot if you
    are not uploading/have not previously uploaded the sample, portion,
    or analyte from which it was derived.  This rule applies to deletes
    as well (see :ref:`label-deleting-entities`.) Exceptions must be
    made to this rule for Programs and Data Bundles elements to be
    uploaded.

**Example bad request:**

.. code-block:: http

    POST /v0/submission/program1/project1 HTTP/1.1
    Host: example.com
    Content-Type: application/json
    X-Auth-Token: MIIDKgYJKoZIhvcNAQcC...
    Accept: application/json

.. code-block:: JavaScript

    [
        {
            "type": "analytes",
            "portions": {
                "submitter_id": "portion-1"
            },
            "analyte_type": "DNA",
            "submitter_id": "analyte-1",
            "amount": 10.98,
            "concentration": 0.14,
            "spectrophotometer_method": "PicoGreen",
            "analyte_type_id": "D"
        },
        {
            "type": "aliquot",
            "analytes": {
                "submitter_id": "analyte-1"
            },
            "submitter_id": "aliquot-1",
            "source_center": "23",
            "concentration": 0.07
        }
    ]

**Example error result:**

.. code-block:: http

    HTTP/1.1 400 BAD REQUEST
    Content-Type: application/json

.. code-block:: JavaScript

    [{
        "code": 400,
        "created_entity_count": 0,
        "entities": [
        {
            "submitter_id": "analyte-1",
            "errors": [
            {
                "keys": ["type"],
                "message": "Invalid entity type: analytes. Did you mean 'analyte'?"
            }
            ],
            "id": "2e1429d5-b2ec-4c02-93ac-207d10b1193c",
            "valid": false,
            "type": "analytes"
        },
        {
            "submitter_id": "aliquot-1",
            "errors": [],
            "id": "6a30b20a-1e38-4c16-8c16-c03ab30f7a11",
            "valid": true,
            "type": "aliquot"
        }
        ],
        "entity_error_count": 1,
        "message": "Transaction aborted due to 1 invalid entity.",
        "success": false,
        "transactional_error_count": 0,
        "transactional_errors": [],
        "updated_entity_count": 0
    }]

**Unsuccessful example response:**

In the second example response, the API returned error code
``400`` and each entity with a list of errors.  The submission API
intends to be as helpful as possible when fixing invalid data
errors and will provide a list of all known errors regarding each
entity.  Ideally, fixing the errors described should result in a
successful uploading and not further errors (i.e. validation is
not short circuited), but this may not always be the case.

.. _label-retrieving-entities:

Retrieving Entities
-------------------

.. _label-replacing-entities:

Replacing Entities
------------------

.. _label-updating-entities:

Updating Entities
-----------------

.. _label-deleting-entities:

Deleting Entities
-----------------
