import pytest
from sheepdog import dictionary
from sheepdog.utils.transforms.graph_to_doc import entity_to_template, is_property_hidden
from sheepdog.utils import _get_links


def test_urls_in_templates_json():
    """Test that urls is in JSON template iff entity is data_file"""
    for label in dictionary.schema:
        if label == 'root':
            continue
        template = entity_to_template(label, file_format='json')
        if dictionary.schema[label]['category'] == 'data_file':
            assert '*urls' in template
        else:
            assert '*urls' not in template


def test_urls_in_templates_tsv():
    """Test that urls is in TSV template iff entity is data_file"""
    for label in dictionary.schema:
        if label == 'root':
            continue
        template = entity_to_template(label, file_format='tsv')
        if dictionary.schema[label]['category'] == 'data_file':
            assert '*urls' in template
        else:
            assert '*urls' not in template


def test_required_fields_have_asterisk_json():
    """Test that required fields in JSON templates have asterisks prepended"""
    exclude_id = True #TODO Check...
    for label in dictionary.schema:
        if label == 'root':
            continue
        template = entity_to_template(label, file_format='json')
        properties = {
            key
            for key in dictionary.schema[label]['properties']
            if not is_property_hidden(key, dictionary.schema[label], exclude_id)
        }
        for key in properties:
            if 'required' in dictionary.schema[label] and key in dictionary.schema[label]['required']:
                marked_key = '*' + key
            else:
                marked_key = key
            assert marked_key in template


def test_required_fields_have_asterisk_tsv():
    """Test that required fields in TSV templates have asterisks prepended"""
    exclude_id = True #TODO Check...
    for label in dictionary.schema:
        if label == 'root':
            continue
        template = entity_to_template(label, file_format='tsv')
        properties = {
            key
            for key in dictionary.schema[label]['properties']
            if not is_property_hidden(key, dictionary.schema[label], exclude_id=True)
        }
        for key in properties:
            if 'required' in dictionary.schema[label] and key in dictionary.schema[label]['required']:
                marked_key = '*' + key
            else:
                marked_key = key
            links = _get_links('tsv', dictionary.schema[label]['links'], exclude_id)
            if key in links:
                for prop in links[key]:
                    assert marked_key + '.' + prop in template
            else:
                assert marked_key in template
