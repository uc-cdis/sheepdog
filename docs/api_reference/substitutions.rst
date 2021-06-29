.. |program_id| replace::
    The program to which the submitter belongs and in which the entities will be
    created. The `program` is the human-readable name, e.g. TCGA.

.. |project_id| replace::
    The project to which the submitter belongs and in which the entities will be
    created. The `project` is the human-readable code, e.g. BRCA.

.. |reqheader_X-Auth-Token| replace::
    The submitter's authorization token as provided by the GDC Authorization
    API.  This is the information that authenticates and authorizes the
    submitter.

.. |reqheader_Content-Type| replace::
    Specify the format of the request payload (this is what the submitter is
    providing).  Must be ``application/json`` or ``application/xml``.

.. |reqheader_Accept| replace::
    Specify the format of the response payload (this is what the submitter will
    get back). Must be ``application/json``, ``application/xml``.

.. |resheader_Content-Type| replace::
    Will be ``application/json`` or ``application/xml`` depending on
    :mailheader: `Accept` header.