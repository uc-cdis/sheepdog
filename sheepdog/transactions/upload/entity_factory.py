"""
Class Factory for creating new upload entities
"""
import psqlgraph

from sheepdog.transactions.upload.entity import UploadEntity
from sheepdog.transactions.upload.sub_entities import NonFileUploadEntity
from sheepdog.transactions.upload.sub_entities import FileUploadEntity
from sheepdog.utils import get_node_category


class UploadEntityFactory:
    """
    Class factory for creating new upload entities based on the
    attributes specified in the given doc.
    """

    @staticmethod
    def create(transaction, doc, config=None):
        """
        Will attempt to parse the  type from the doc and check the dictionary
        for that node type's category.

        Will then return an instance of UploadEntity or one of its
        subclasses based on the discovered category.
        """
        if not isinstance(doc, dict):
            # We cannot determine category, just create base class
            # NOTE: This error with doc will get recorded using record_error()
            #       when the parse() function gets called to support
            #       more helpful debugging
            return UploadEntity(transaction, config)

        # Remove asterisks from dict keys
        for key in list(doc):
            doc[key.lstrip("*")] = doc.pop(key)

        node_type = doc.get("type")
        node_category = get_node_category(node_type)

        if node_category in UploadEntity.DATA_FILE_CATEGORIES:
            return FileUploadEntity(transaction, config)
        else:
            return NonFileUploadEntity(transaction, config)
