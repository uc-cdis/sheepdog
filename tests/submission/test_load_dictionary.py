import unittest
from mock import patch
from sheepdog.api import app_load_dictionary
from dictionaryutils import DataDictionary


class TestExample(unittest.TestCase):
    def test_datadictionary_called(self):
        with patch('dictionaryutils.DataDictionary') as mocked_object:
            url = 'fake_url'
            app_load_dictionary(url)
            mocked_object.assert_called_once_with(url=url)

    def test_datadictionary_valid(self):
        with patch('dictionaryutils.DataDictionary') as mocked_object:
            url = 'fake_url'
            mocked_object = 'something'
            assert app_load_dictionary(url)