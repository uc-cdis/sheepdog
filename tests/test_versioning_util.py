import unittest

import pytest as pytest

from sheepdog.utils.versioning import VersionHelper


class VersionHelperTest(unittest.TestCase):

    @pytest.fixture(autouse=True)
    def init(self, index_client):
        self.vh = VersionHelper(index_client)

    def test_add_node_revision(self):
        self.fail("Not Implemented")

    def test_bulk_node_revisions(self):
        self.fail("Not Implemented")


if __name__ == '__main__':
    unittest.main()
