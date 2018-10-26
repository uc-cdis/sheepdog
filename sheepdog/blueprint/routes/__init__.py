"""
List routes to be added to the blueprint in ``sheepdog.blueprint``. Each
route is constructed with the ``new_route`` function from
``sheepdog.blueprint.routes.route_utils``.
"""

from sheepdog.blueprint.routes import views
from openapi.definitions import tag_dry_run, tag_dictionary, tag_program, tag_project, tag_export, tag_file, tag_entity


def new_route(rule, view_func, endpoint=None, methods=None, options=None, swagger=None, schema=None):
    """
    Construct a dictionary representation of a URL rule to be added to the
    blueprint.

    Args:
        rule (str): the path for the URL
        view_func (callable): function to render the page

    Keyword Args:
        endpoint (str): endpoint name (internal Flask usage)
        methods (list[str]): list of methods the rule should handle (GET, etc.)
        options (dict): options to pass as keyword args to ``add_url_rule``

    Return:
        dict: dictionary containing the above information for the route
    """
    if options is None:
        options = {}
    if methods is not None:
        options['methods'] = methods
    return {
        'rule': rule,
        'view_func': view_func,
        'endpoint': endpoint,
        'options': options,
        'swagger': swagger,
        'schema': schema # Swagger schema definitions (defined in openapi/definitions)
    }

routes = [
    new_route(
        '/',
        views.get_programs,
        methods=['GET'],
        swagger={
            'summary': 'Get the programs',
            'tags': [tag_program]
        },
        schema={
            '200': 'schema_links' # description of response when status code is 200
        }
    ),
    new_route(
        '/',
        views.root_create,
        methods=['PUT', 'POST', 'PATCH'],
        swagger={
            'summary': 'Create a program',
            'tags': [tag_program]
        },
        schema={
            'body': 'schema_program' # description of input body
        }
    ),
    new_route(
        '/_dictionary',
        views.get_dictionary,
        methods=['GET'],
        swagger={
            'summary': 'Get the dictionary schema',
            'tags': [tag_dictionary]
        },
        schema={
            '200': 'schema_links'
        }
    ),
    new_route(
        '/_dictionary/<entry>',
        views.program.project.get_dictionary_entry,
        methods=['GET'],
        swagger={
            'summary': 'Get the dictionary schema for an entity',
            'tags': [tag_dictionary]
        },
        schema={
            '200': 'schema_entity'
        }
    ),
    new_route(
        '/<program>',
        views.program.get_projects,
        methods=['GET'],
        swagger={
            'summary': 'Get the projects',
            'tags': [tag_project]
        },
        schema={
            '200': 'schema_links'
        }
    ),
    new_route(
        '/<program>',
        views.program.create_project,
        methods=['PUT', 'POST', 'PATCH'],
        swagger={
            'summary': 'Create a project',
            'tags': [tag_project]
        },
        schema={
            'body': 'schema_project'
        }
    ),
    new_route(
        '/<program>',
        views.program.delete_program,
        methods=['DELETE'],
        swagger={
            'summary': 'Delete a program',
            'tags': [tag_program]
        }
    ),
    new_route(
        '/<program>/<project>',
        views.program.project.delete_project,
        methods=['DELETE'],
        swagger={
            'summary': 'Delete a project',
            'tags': [tag_project]
        }
    ),
    new_route(
        '/<program>/<project>',
        views.program.project.create_viewer('POST'),
        endpoint='create_entities',
        methods=['POST'],
        swagger={
            'summary': 'Create entities',
            'description': 'Create any valid entities specified in the request body.',
            'tags': [tag_entity]
        },
        schema={
            'body': 'schema_entity'
        }
    ),
    new_route(
        '/<program>/<project>',
        views.program.project.create_viewer('PUT'),
        endpoint='update_entities',
        methods=['PUT'],
        swagger={
            'summary': 'Update entities',
            'description': 'Update the entity specified in the request body.',
            'tags': [tag_entity]
        },
        schema={
            'body': 'schema_entity'
        }
    ),
    new_route(
        '/<program>/<project>/_dry_run',
        views.program.project.create_viewer('POST', dry_run=True),
        endpoint='create_entities_dry_run',
        methods=['POST'],
        swagger={
            'summary': 'Create entities',
            'description': 'Create any valid entities specified in the request body.',
            'tags': [tag_dry_run]
        },
        schema={
            'body': 'schema_entity'
        }
    ),
    new_route(
        '/<program>/<project>/_dry_run',
        views.program.project.create_viewer('PUT', dry_run=True),
        endpoint='update_entities_dry_run',
        methods=['PUT'],
        swagger={
            'summary': 'Update entities',
            'description': 'Update the entity specified in the request body.',
            'tags': [tag_dry_run]
        },
        schema={
            'body': 'schema_entity'
        }
    ),
    new_route(
        '/<program>/<project>/bulk',
        views.program.project.create_viewer('POST', bulk=True),
        endpoint='bulk_create_entities',
        methods=['POST'],
        swagger={
            'summary': 'Create entities in bulk',
            'description': 'Bulk creation of entities.',
            'tags': [tag_entity]
        },
        schema={
            'body': 'schema_entity_bulk'
        }
    ),
    new_route(
        '/<program>/<project>/bulk',
        views.program.project.create_viewer('PUT', bulk=True),
        endpoint='bulk_update_entities',
        methods=['PUT'],
        swagger={
            'summary': 'Update entities in bulk',
            'description': 'Bulk update of entities.',
            'tags': [tag_entity]
        },
        schema={
            'body': 'schema_entity_bulk'
        }
    ),
    new_route(
        '/<program>/<project>/bulk/_dry_run',
        views.program.project.create_viewer('POST', bulk=True, dry_run=True),
        endpoint='bulk_create_entities_dry_run',
        methods=['POST'],
        swagger={
            'summary': 'Create entities in bulk',
            'description': 'Bulk creation of entities.',
            'tags': [tag_dry_run]
        },
        schema={
            'body': 'schema_entity_bulk'
        }
    ),
    new_route(
        '/<program>/<project>/bulk/_dry_run',
        views.program.project.create_viewer('PUT', bulk=True, dry_run=True),
        endpoint='bulk_update_entities_dry_run',
        methods=['PUT'],
        swagger={
            'summary': 'Update entities in bulk',
            'description': 'Bulk update of entities.',
            'tags': [tag_dry_run]
        },
        schema={
            'body': 'schema_entity_bulk'
        }
    ),
    new_route(
        '/<program>/<project>/_dictionary',
        views.program.project.get_project_dictionary,
        methods=['GET'],
        swagger={
            'summary': 'Get the dictionary schema for entities of a project',
            'description': 'Return links to the project level JSON schema definitions.',
            'tags': [tag_dictionary]
        },
        schema={
            '200': 'schema_links'
        }
    ),
    new_route(
        '/<program>/<project>/_dictionary/<entry>',
        views.program.project.get_project_dictionary_entry,
        methods=['GET'],
        swagger={
            'summary': 'Get the dictionary schema for an entity of a project',
            'description': 'Get the dictionary entry for a specific project.',
            'tags': [tag_dictionary]
        },
        schema={
            '200': 'schema_entity'
        }
    ),
    new_route(
        '/<program>/<project>/entities/<entity_id_string>',
        views.program.project.get_entities_by_id,
        methods=['GET'],
        swagger={
            'summary': 'Get entities by ID',
            'description': 'Retrieve existing entities by ID.',
            'tags': [tag_entity]
        },
        schema={
            '200': 'schema_entity_list'
        }
    ),
    new_route(
        '/<program>/<project>/entities/<ids>',
        views.program.project.create_delete_entities_viewer(),
        endpoint='delete_entities',
        methods=['DELETE'],
        swagger={
            'summary': 'Delete entities',
            'description': 'Delete existing entities. Deletions that would leave nodes without parents, i.e. nodes that do not have an entity from which they were derived, are not allowed.',
            'tags': [tag_entity]
        }
    ),
    new_route(
        '/<program>/<project>/entities/_dry_run/<ids>',
        views.program.project.create_delete_entities_viewer(dry_run=True),
        endpoint='delete_entities_dry_run',
        methods=['DELETE'],
        swagger={
            'summary': 'Delete entities',
            'description': 'Delete existing entities. Deletions that would leave nodes without parents, i.e. nodes that do not have an entity from which they were derived, are not allowed.',
            'tags': [tag_dry_run]
        }
    ),
    new_route(
        '/<program>/<project>/export',
        views.program.project.export_entities,
        methods=['GET', 'POST'],
        swagger={
            'summary': 'Export entities',
            'produces': ['application/json'],
            'tags': [tag_export]
        }
    ),
    new_route(
        '/<program>/<project>/files/<file_uuid>',
        views.program.project.create_files_viewer(),
        endpoint='file_operations_get',
        methods=['GET'],
        swagger={
            'summary': 'Get a data file',
            'description': 'Get a data file from object storage',
            'parameters': [{
                'name': 'uploadId',
                'in' : 'query',
                'description': 'to list parts',
                'type': 'string'
            }],
            'tags': [tag_file]
        }
    ),
    new_route(
        '/<program>/<project>/files/<file_uuid>/_dry_run',
        views.program.project.create_files_viewer(dry_run=True),
        endpoint='file_operations_get_dry_run',
        methods=['GET'],
        swagger={
            'summary': 'Get a data file',
            'description': 'Get a data file from object storage',
            'parameters': [{
                'name': 'uploadId',
                'in' : 'query',
                'description': 'to list parts',
                'type': 'string'
            }],
            'tags': [tag_dry_run]
        }
    ),
    new_route(
        '/<program>/<project>/files/<file_uuid>',
        views.program.project.create_files_viewer(),
        endpoint='file_operations_put',
        methods=['PUT'],
        swagger={
            'summary': 'Upload a data file',
            'description': 'Upload data using single PUT. The request body should contain binary data of the file.',
            'parameters': [
                {
                    'name': 'partNumber',
                    'in' : 'query',
                    'description': 'to upload part (use with uploadId)',
                    'type': 'string'
                },
                {
                    'name': 'uploadId',
                    'in' : 'query',
                    'description': 'to upload part (use with partNumber)',
                    'type': 'string'
                }
            ],
            'tags': [tag_file]
        }
    ),
    new_route(
        '/<program>/<project>/files/<file_uuid>/_dry_run',
        views.program.project.create_files_viewer(dry_run=True),
        endpoint='file_operations_put_dry_run',
        methods=['PUT'],
        swagger={
            'summary': 'Upload a data file',
            'description': 'Upload data using single PUT. The request body should contain binary data of the file.',
            'parameters': [
                {
                    'name': 'partNumber',
                    'in' : 'query',
                    'description': 'to upload part (use with uploadId)',
                    'type': 'string'
                },
                {
                    'name': 'uploadId',
                    'in' : 'query',
                    'description': 'to upload part (use with partNumber)',
                    'type': 'string'
                }
            ],
            'tags': [tag_dry_run]
        }
    ),
    new_route(
        '/<program>/<project>/files/<file_uuid>',
        views.program.project.create_files_viewer(),
        endpoint='file_operations_post',
        methods=['POST'],
        swagger={
            'summary': 'Upload data by multipart upload',
            'description': 'Upload data by multipart upload',
            'parameters': [
                {
                    'name': 'uploads',
                    'in' : 'query',
                    'description': 'to initiate multipart upload',
                    'type': 'string'
                },
                {
                    'name': 'uploadId',
                    'in' : 'query',
                    'description': 'to complete multipart upload',
                    'type': 'string'
                }
            ],
            'tags': [tag_file]
        }
    ),
    new_route(
        '/<program>/<project>/files/<file_uuid>/_dry_run',
        views.program.project.create_files_viewer(dry_run=True),
        endpoint='file_operations_post_dry_run',
        methods=['POST'],
        swagger={
            'summary': 'Upload data by multipart upload',
            'description': 'Upload data by multipart upload',
            'parameters': [
                {
                    'name': 'uploads',
                    'in' : 'query',
                    'description': 'to initiate multipart upload',
                    'type': 'string'
                },
                {
                    'name': 'uploadId',
                    'in' : 'query',
                    'description': 'to complete multipart upload',
                    'type': 'string'
                }
            ],
            'tags': [tag_dry_run]
        }
    ),
    new_route(
        '/<program>/<project>/files/<file_uuid>',
        views.program.project.create_files_viewer(),
        endpoint='file_operations_delete',
        methods=['DELETE'],
        swagger={
            'summary': 'Delete a data file',
            'description': 'Delete molecular data from object storage.',
            'parameters': [{
                'name': 'uploadId',
                'in' : 'query',
                'description': 'to abort a multipart upload',
                'type': 'string'
            }],
            'tags': [tag_file]
        }
    ),
    new_route(
        '/<program>/<project>/files/<file_uuid>/_dry_run',
        views.program.project.create_files_viewer(dry_run=True),
        endpoint='file_operations_delete_dry_run',
        methods=['DELETE'],
        swagger={
            'summary': 'Delete a data file',
            'description': 'Delete molecular data from object storage.',
            'parameters': [{
                'name': 'uploadId',
                'in' : 'query',
                'description': 'to abort a multipart upload',
                'type': 'string'
            }],
            'tags': [tag_dry_run]
        }
    ),
    new_route(
        '/<program>/<project>/manifest',
        views.program.project.get_manifest,
        methods=['GET'],
        swagger={
            'summary': 'Get a manifest of data files',
            'tags': [tag_file]
        }
    ),
    new_route(
        '/<program>/<project>/open',
        views.program.project.create_open_project_viewer(),
        endpoint='open_project',
        methods=['PUT', 'POST'],
        swagger={
            'summary': 'Open a project',
            'tags': [tag_project]
        }
    ),
    new_route(
        '/<program>/<project>/open/_dry_run',
        views.program.project.create_open_project_viewer(dry_run=True),
        endpoint='open_project_dry_run',
        methods=['PUT', 'POST'],
        swagger={
            'summary': 'Open a project',
            'tags': [tag_dry_run]
        }
    ),
    new_route(
        '/<program>/<project>/release',
        views.program.project.create_release_project_viewer(),
        endpoint='release_project',
        methods=['PUT', 'POST'],
        swagger={
            'summary': 'Release a project',
            'tags': [tag_project]
        }
    ),
    new_route(
        '/<program>/<project>/release/_dry_run',
        views.program.project.create_release_project_viewer(dry_run=True),
        endpoint='release_project_dry_run',
        methods=['PUT', 'POST'],
        swagger={
            'summary': 'Release a project',
            'tags': [tag_dry_run]
        }
    ),
    new_route(
        '/<program>/<project>/review',
        views.program.project.create_review_project_viewer(),
        endpoint='review_project',
        methods=['PUT', 'POST'],
        swagger={
            'summary': 'Review a project',
            'tags': [tag_project]
        }
    ),
    new_route(
        '/<program>/<project>/review/_dry_run',
        views.program.project.create_review_project_viewer(dry_run=True),
        endpoint='review_project_dry_run',
        methods=['PUT', 'POST'],
        swagger={
            'summary': 'Review a project',
            'tags': [tag_dry_run]
        }
    ),
    new_route(
        '/<program>/<project>/submit',
        views.program.project.create_submit_project_viewer(),
        endpoint='submit_project',
        methods=['PUT', 'POST'],
        swagger={
            'summary': 'Submit a project',
            'description': 'Submit a project. Submitting a project means that all metadata that *currently* exists in the project can be made public in every index built after the project is released.',
            'tags': [tag_project]
        }
    ),
    new_route(
        '/<program>/<project>/submit/_dry_run',
        views.program.project.create_submit_project_viewer(dry_run=True),
        endpoint='submit_project_dry_run',
        methods=['PUT', 'POST'],
        swagger={
            'summary': 'Submit a project',
            'description': 'Submit a project. Submitting a project means that all metadata that *currently* exists in the project can be made public in every index built after the project is released.',
            'tags': [tag_dry_run]
        }
    ),
    new_route(
        '/<program>/<project>/template',
        views.program.project.get_project_templates,
        methods=['GET'],
        swagger={
            'summary': 'Get templates for all entity types of a project',
            'tags': [tag_dictionary]
        }
    ),
    new_route(
        '/<program>/<project>/template/<entity>',
        views.program.project.get_project_template,
        methods=['GET'],
        swagger={
            'summary': 'Get a template for an entity type of a project',
            'tags': [tag_dictionary]
        }
    ),
    new_route(
        '/<program>/<project>/transactions/<int:transaction_id>/close',
        views.program.project.close_transaction,
        methods=['PUT', 'POST'],
        swagger={
            'summary': 'Close a transaction',
            'tags': [tag_dry_run]
        }
    ),
    new_route(
        '/<program>/<project>/transactions/<int:transaction_id>/commit',
        views.program.project.commit_dry_run_transaction,
        methods=['PUT', 'POST'],
        swagger={
            'summary': 'Commit a dry run transaction.',
            'tags': [tag_dry_run]
        }
    ),
    new_route(
        '/<program>/<project>/upload_manifest',
        views.program.project.get_manifest,
        methods=['GET'],
        swagger={
            'summary': 'Get a manifest of data files',
            'tags': [tag_file]
        }
    ),
    new_route(
        '/<program>/<project>/xml/biospecimen/bcr',
        views.program.project.create_biospecimen_viewer(),
        endpoint='update_entities_biospecimen',
        methods=['PUT'],
        swagger={
            'summary': 'Update Biospecimen Supplement entities',
            'consumes': ['multipart/form-data'],
            'parameters': [{
                'name': 'file',
                'in' : 'formData',
                'description': 'BRC XML file',
                'type': 'file',
                'required': True
            }],
            'tags': [tag_entity]
        }
    ),
    new_route(
        '/<program>/<project>/xml/biospecimen/bcr/_dry_run',
        views.program.project.create_biospecimen_viewer(dry_run=True),
        endpoint='update_entities_biospecimen_dry_run',
        methods=['PUT'],
        swagger={
            'summary': 'Update Biospecimen Supplement entities',
            'consumes': ['multipart/form-data'],
            'parameters': [{
                'name': 'file',
                'in' : 'formData',
                'description': 'BRC XML file',
                'type': 'file',
                'required': True
            }],
            'tags': [tag_dry_run]
        }
    ),
    new_route(
        '/<program>/<project>/xml/clinical/bcr',
        views.program.project.create_clinical_viewer(),
        endpoint='update_entities_clinical_bcr',
        methods=['PUT'],
        swagger={
            'summary': 'Update Clinical Supplement entities',
            'consumes': ['multipart/form-data'],
            'parameters': [{
                'name': 'file',
                'in' : 'formData',
                'description': 'BRC XML file',
                'type': 'file',
                'required': True
            }],
            'tags': [tag_entity]
        }
    ),
    new_route(
        '/<program>/<project>/xml/clinical/bcr/_dry_run',
        views.program.project.create_clinical_viewer(dry_run=True),
        endpoint='update_entities_clinical_bcr_dry_run',
        methods=['PUT'],
        swagger={
            'summary': 'Update Clinical Supplement entities',
            'consumes': ['multipart/form-data'],
            'parameters': [{
                'name': 'file',
                'in' : 'formData',
                'description': 'BRC XML file',
                'type': 'file',
                'required': True
            }],
            'tags': [tag_dry_run]
        }
    ),
    new_route(
        '/template',
        views.get_templates,
        methods=['GET'],
        swagger={
            'summary': 'Get templates for all entity types',
            'tags': [tag_dictionary]
        }
    ),
    new_route(
        '/template/<entity>',
        views.get_template,
        methods=['GET'],
        swagger={
            'summary': 'Get a template for an entity type',
            'tags': [tag_dictionary]
        }
    ),
    new_route(
        '/validation/upload_manifest',
        views.validate_upload_manifest,
        methods=['POST'],
        swagger={
            'summary': 'Validate a manifest of data files',
            'tags': [tag_file]
        },
        schema={
            '200': 'schema_error_list'
        }
    ),
    new_route(
        '/admin/<program>/<project>/files/<file_uuid>/reassign',
        views.program.project.create_files_viewer(reassign=True),
        endpoint='reassign_file_operations_admin',
        methods=['PUT'],
        swagger={
            'summary': 'Reassign the S3 URL of a data file',
            'description': 'Manually (re)assign the S3 url for a given node.',
            'tags': [tag_file]
        }
    ),
    new_route(
        '/admin/<program>/<project>/entities/<ids>/to_delete/<to_delete>',
        views.program.project.create_delete_entities_viewer(),
        endpoint='delete_entities_admin',
        methods=['DELETE'],
        swagger={
            'summary': 'Delete entities',
            'description': 'Delete existing entities. Deletions that would leave nodes without parents, i.e. nodes that do not have an entity from which they were derived, are not allowed.',
            'tags': [tag_entity]
        }
    ),
]
