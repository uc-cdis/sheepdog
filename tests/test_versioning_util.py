import unittest

import pytest as pytest

from sheepdog.utils.versioning import IndexVersionHelper


class VersionHelperTest(unittest.TestCase):

    @pytest.fixture(autouse=True)
    def init(self, index_client):
        self.vh = IndexVersionHelper(index_client)

    def test_release_node(self):
        self.fail("Not Implemented")

    def test_add_node_revision(self):
        self.fail("Not Implemented")

    def test_bulk_node_revisions(self):
        self.fail("Not Implemented")


if __name__ == '__main__':
    unittest.main()
