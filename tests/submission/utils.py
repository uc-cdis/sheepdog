import os
import re
import uuid

import indexclient

from gdcdatamodel import models


DATA_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data')

# https://stackoverflow.com/questions/373194/python-regex-for-md5-hash
re_md5 = re.compile(r'(i?)(?<![a-z0-9])[a-f0-9]{32}(?![a-z0-9])')

data_fnames = [
    'experiment.json',
    'case.json',
    'sample.json',
    'aliquot.json',
    'demographic.json',
    'diagnosis.json',
    'exposure.json',
    'treatment.json',
]

PATH = '/v0/submission/graphql'
BLGSP_PATH = '/v0/submission/CGCI/BLGSP/'
BRCA_PATH = '/v0/submission/TCGA/BRCA/'


def put_entity_from_file(
        client, file_path, submitter, put_path=BLGSP_PATH, validate=True):
    with open(os.path.join(DATA_DIR, file_path), 'r') as f:
        entity = f.read()
    r = client.put(put_path, headers=submitter(put_path, 'put'), data=entity)
    if validate:
        assert r.status_code == 200, r.data
    return r


def reset_transactions(pg_driver):
    with pg_driver.session_scope() as s:
        s.query(models.submission.TransactionSnapshot).delete()
        s.query(models.submission.TransactionDocument).delete()
        s.query(models.submission.TransactionLog).delete()
