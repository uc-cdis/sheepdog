"""
Utility functions for handling common tasks in versioning
"""


class IndexVersionHelper:

    def __init__(self, index_client):
        """
        :type index_client: indexclient.client.IndexClient
        :param index_client:
        """
        self.index_client = index_client

    def add_node_version(self, did):
        """
        Queries IndexD to get the latest version for the specified did,
        updates the version number for this revision and finally adds the revision
        :type did: str
        :param did: Digital document id
        :rtype: indexclient.client.IndexClient
        :return: a revised version of the specified index id containing the latest version number
        """
        # get latest revision for this node
        revision = self.index_client.get(did)

        if revision and revision.version:
            # create revision
            revision = self.index_client.add_version(did)
        elif revision.version is None:
            revision.version = "1"
            revision.patch()
        return revision

    def release_node(self, gdc_release_number, gdc_node_id):
        """
        Performs a GDC release action on a given node

        * Using the node id, retrieve all versions from indexd
        * filter out the latest unversioned and the latest version number
            * the latest unversioned is an entry with version set to None (there should be only one of this)
            * the latest version number is the highest value of the version field from all entries
            with version not None
        Args:
            gdc_release_number (str): The GDC release number, gotten from GDC
            gdc_node_id (str): The GDC node_id of the node to be released

        Return:
            bool: True if release happened, else False
        """

        versions = self.index_client.list_versions(gdc_node_id)  # type: list[indexclient.client.Document}
        latest_version_number = 0
        latest_unversioned = None

        for version in versions:
            if version.version is None:
                # there can only be one of this
                latest_unversioned = version
            elif int(version) > latest_version_number:
                latest_version_number = int(version.version)

        if latest_unversioned is not None:
            latest_unversioned.version = latest_version_number + 1
            latest_unversioned.metadata["gdc_release_number"] = gdc_release_number
            latest_version_number.patch()

    def add_bulk_revisions(self, did_list):
        """
        :type did_list: list[str]
        :param did_list: list of document ids
        :rtype: list[indexclient.client.IndexClient]
        :return: list of updated revisions
        """
        revisions = []
        for did in did_list:
            revisions.append(self.add_node_revision(did))
        return revisions
