import pytest
from sheepdog import dictionary
from sheepdog.utils.transforms.graph_to_doc import entity_to_template


def test_urls_in_templates_json():
    """Test that urls is in JSON template iff entity is data_file"""
    for label in dictionary.schema:
        if label == 'root':
            continue
        template = entity_to_template(label, file_format='json')
        if dictionary.schema[label]['category'] == 'data_file':
            assert 'urls' in template
        else:
            assert 'urls' not in template

def test_urls_in_templates_tsv():
    """Test that urls is in TSV template iff entity is data_file"""
    for label in dictionary.schema:
        if label == 'root':
            continue
        template = entity_to_template(label, file_format='tsv')
        if dictionary.schema[label]['category'] == 'data_file':
            assert 'urls' in template
        else:
            assert not 'urls' in template
