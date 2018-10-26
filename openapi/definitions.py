# names of sections in the Swagger doc
tag_dry_run = 'dry run (transactions are not committed)'
tag_dictionary = 'dictionary'
tag_program = 'program'
tag_project = 'project'
tag_export = 'export'
tag_file = 'file'
tag_entity = 'entity'


definitions = {
    'schema_links': {
        'type': 'object',
        'required': [
            'links'
        ],
        'properties': {
            'links': {
                'type': 'array',
                'items': {
                    'type': 'string'
                }
            }
        }
    },
    'schema_program': {
        'type': 'object',
        'required': [
            'name',
            'dbgap_accession_number'
        ],
        'properties': {
            'name': {
                'type': 'string'
            },
            'type': {
                'type': 'string'
            },
            'dbgap_accession_number': {
                'type': 'string'
            }
        }
    },
    'schema_project': {
        'type': 'object',
        'required': [
            'name',
            'code'
        ],
        'properties': {
            'name': {
                'type': 'string'
            },
            'type': {
                'type': 'string'
            },
            'code': {
                'type': 'string'
            },
            'dbgap_accession_number': {
                'type': 'string'
            },
            'investigator_name': {
                'type': 'string'
            }
        }
    },
    'schema_entity': {
        'type': 'object',
        'properties': {
            'type': {
                'type': 'string'
            },
            '...fields specific to this entity': {
                'type': 'string'
            }
        }
    },
    'schema_entity_list': {
        'type': 'array',
        'items': {
            '$ref': '#/definitions/schema_entity'
        }
    },
    'schema_entity_bulk': {
        'type': 'object',
        'required': [
            'name',
            'doc_format',
            'doc'
        ],
        'properties': {
            'name': {
                'type': 'string'
            },
            'doc_format': {
                'type': 'string'
            },
            'doc': {
                'type': 'string'
            }
        }
    },
    'schema_error_list': {
        'type': 'array',
        'items': {
            'type': 'string'
        }
    }
}