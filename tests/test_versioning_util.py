import unittest
import uuid

import hashlib
import pytest as pytest

from sheepdog.utils.versioning import IndexVersionHelper


class VersionHelperTest(unittest.TestCase):

    @pytest.fixture(autouse=True)
    def init(self, index_client):
        """
        Args:
            index_client (indexclient.client.IndexClient):
        """
        self.client = index_client
        self.vh = IndexVersionHelper(index_client)

    def create_dummy_entry(self, add_version_number=True):
        did = str(uuid.uuid4())

        md5 = hashlib.md5()
        md5.update(did)
        # add a new node index
        hashes = {'md5': md5.hexdigest()}
        doc = self.client.create(
            hashes=hashes,
            size=5,
            file_name=did + "_main_version.ftl",
            urls=[]
        )

        # give it a version number
        if add_version_number:
            doc.version = "1"
            doc.patch()

        return doc

    def add_dummy_version(self, did):
        # add a new version
        sha256 = hashlib.sha256()
        ver = self.vh.add_node_version(
            did,
            hashes={"sha256": sha256.hexdigest()},
            size=22,
            file_name=did + "_dummy_version.ftl",
            urls=["s3://IamGoingToMars/{}_marsians.ftl".format(str(uuid.uuid4()))])

        return ver

    def test_release_node(self):
        # add a new node index
        doc = self.create_dummy_entry()
        print(doc.to_json())
        # add a new version
        ver = self.add_dummy_version(doc.did)
        released = self.vh.release_node("11", doc.did)
        self.assertTrue(released)

        # validation, get the latest version
        latest = self.client.get_latest_version(doc.did)
        self.assertEquals(latest.version, "2")

        # TODO: fails cos IndexD does not allow updating metadata
        print(latest.to_json())
        self.assertEquals(latest.metadata["release_number"], "11")
        self.assertEquals(latest.metadata["file_state"], "submitted")

    def test_add_new_node_revision(self):
        # add a new node index
        doc = self.create_dummy_entry()

        # add a new version
        ver = self.add_dummy_version(doc.did)

        self.assertIsNone(ver.version)
        self.assertEquals(doc.baseid, ver.baseid)
        self.assertFalse(ver.hashes.has_key("md5"))


if __name__ == '__main__':
    unittest.main()
