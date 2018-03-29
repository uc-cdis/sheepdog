"""
Utility functions for handling common tasks in versioning
"""
from cdislogging import get_logger
from indexclient.client import Document, IndexClient


class IndexVersionHelper:

    def __init__(self, index_client, psql_driver):
        """
        Args:
            index_client (indexclient.client.IndexClient):
            psql_driver (psqlgraph.PsqlGraphDriver)
        """
        self.logger = get_logger("versioning.IndexVersionHelper")
        self.g = psql_driver
        self.index_client = index_client

    def add_node_version(self, family_member_gdc_id, hashes, size, file_name=None, urls=None, metadata=None):
        """
        Adds a node version to indexd

        Args:
            family_member_gdc_id (str): the gdc id of a family of nodes
            hashes (dict): hashes for the new version, this is required to have at least one hash entry
            size (int): file size
            file_name (str): name of the file
            urls (lst[str]): URLs for this file
            metadata (dict): key value pairs
        Returns:
            indexclient.client.Document: the new version
        """

        index_json = dict(hashes=hashes, size=size, file_name=file_name, urls=urls, metadata=metadata, form="object")

        # dummy document object, used merely for passing values
        versioned_doc = Document(None, None, index_json)

        # get latest revision for this node
        # this is to ensure we don't add multiple unversioned entries ito indexd
        revision = self.index_client.get(family_member_gdc_id)

        if revision and revision.version:
            # create a version
            revision = self.index_client.add_version(family_member_gdc_id, versioned_doc)
        elif revision.version is None:
            # there's already a version than can be updated as often as desired

            # TODO: update document entries - clarify if replace all fields is the way to go
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
            tuple[bool, str]: True if release happened, else False, the latest gdc_release_number
        """

        versions = self.index_client.list_versions(gdc_node_id)  # type: list[indexclient.client.Document}
        latest_version_number = 0
        latest_version = None
        latest_unversioned = None

        for version in versions:
            if version.version is None:
                # there can only be one of this
                latest_unversioned = version
            elif int(version.version) > latest_version_number:
                latest_version_number = int(version.version)
                latest_version = version

        if latest_unversioned is not None:
            latest_unversioned.version = str(latest_version_number + 1)
            latest_unversioned.metadata["release_number"] = gdc_release_number
            latest_unversioned.patch()
            self.logger.info("Release updated for: {}".format(latest_unversioned.did))
            return True, gdc_release_number
        return False, latest_version.metadata.get("release_number")

    def get_unversioned_nodes(self):

        pass

    def do_project_release(self, release_number, project_id):
        """
        For the given project,
            * look up all nodes that are in the submitted state
            * For each of these nodes perform a release
            * Update the state of the node to released
        Args:
            release_number(str): The currently active release number
            project_id(str): project id of nodes being released
        Returns:
             int: count of nodes successfully released
        """
        # get unreleased project nodes

        with self.g.session_scope() as _:

            un_released_nodes = self.g.nodes().props(state="submitted", project_id=project_id).all()
            for node in un_released_nodes:
                released, latest_release_number = self.release_node(release_number, node.id)
                if latest_release_number == release_number:
                    # entry has already been released
                    node.props[""]
                    pass

    def do_release(self, gdc_release_number):
        """
        WIP: go through all entries on indexd and perform a node release
        Args:
            gdc_release_number (str): the current gdc release number

        Returns:
             int: number of nodes updated
        """
        self.logger.info("Performing GDC Release: {}".format(gdc_release_number))
        unversioned = self.index_client.list_with_params(params={"version": 'None'})
        released_count = 0
        for version in unversioned:
            if self.release_node(gdc_release_number, version.did):
                released_count += 1
        return released_count


# Just Testing
if __name__ == '__main__':
    client = IndexClient('http://172.21.13.60','v0', ('dev_indexd', '7"s^=eLIqKbw%qA8'))
    vh = IndexVersionHelper(client)

    print(vh.do_release("12"))
