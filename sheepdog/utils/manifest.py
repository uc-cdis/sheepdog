"""
TODO
"""

from jsonschema import Draft4Validator

from sheepdog.errors import UserError
from sheepdog.utils.transforms.graph_to_doc import ExportFile

UPLOAD_MANIFEST_SCHEMA = {
    "title": "Manifest Schema",
    "type": "object",
    "additionalProperties": True,
    "properties": {
        "files": {
            "items": {
                "type": "object",
                "properties": {
                    "id": {
                        "type": "string",
                        "pattern": "^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$",
                    },
                    "file_name": {"type": "string"},
                    "local_file_path": {"type": "string"},
                    "file_size": {"type": "integer"},
                    "md5sum": {"type": "string", "pattern": "^[a-f0-9]{32}$"},
                    "type": {"type": "string"},
                    "project_id": {"type": "string"},
                },
                "anyOf": [
                    {"required": ["id", "file_name", "project_id"]},
                    {"required": ["id", "local_file_path", "project_id"]},
                ],
            }
        }
    },
}


def get_manifest(program, project, ids):
    """
    Use the ExportFile exporter to create a json export of the file. This json
    export is used as the base for the manifest.

    :return: a list of file dictionary objects
    """
    errors = []
    exporter = ExportFile(program=program, project=project, ids=ids)
    # Verify that all nodes are actually data_files
    for node in exporter.nodes:
        if node._dictionary["category"] not in ["data_file"]:
            msg = "{} {} is not a data file.".format(node.label, node.node_id)
            errors.append(msg)
    if errors:
        raise UserError(". ".join(errors))

    # The exporter returns files nested under their types, so flatten
    # it here and add the local_file_path
    files = [
        dict(local_file_path=doc.get("file_name"), **doc)
        for file_type in list(exporter.result.values())
        for doc in file_type
    ]

    return files


def validate_upload_manifest(manifest, schema=None):
    """Generate a list of errors found in JSON Schema validation."""
    if schema is None:
        schema = UPLOAD_MANIFEST_SCHEMA
    return [e.message for e in Draft4Validator(schema).iter_errors(manifest)]
