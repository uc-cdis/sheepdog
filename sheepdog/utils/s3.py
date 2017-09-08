"""
TODO
"""

import socket
import urlparse

import boto
import flask

from sheepdog.globals import (
    UPLOADING_PARTS,
)


def get_s3_conn(host):
    """
    Get a connection to a given storage host based on configuration in the
    current app context.
    """
    config = flask.current_app.config["STORAGE"]["s3"]
    return boto.connect_s3(
        config["keys"][host]["access_key"], config["keys"][host]["secret_key"],
        **config["kwargs"][host]
    )


def get_s3_hosts():
    return set(
        ip
        for (_, _, _, _, (ip, _))
        in socket.getaddrinfo(
            flask.current_app.config['SUBMISSION']['host'], 80
        )
    )


def get_submission_bucket():
    conn = get_s3_conn(flask.current_app.config['SUBMISSION']['host'])
    return conn.get_bucket(flask.current_app.config['SUBMISSION']['bucket'])


def make_s3_request(project_id, uuid, data, args, headers, method, action):
    key_name = project_id + '/' + uuid
    bucket = None
    if action in UPLOADING_PARTS:
        upload_id = urlparse.parse_qs(args)['uploadId'][0]
        for ip in get_s3_hosts():
            bucket = get_submission_bucket()
            res = bucket.connection.make_request(
                'GET', bucket=bucket.name, key=key_name,
                data="", query_args="uploadId={}".format(upload_id),
                headers=headers)
            if res.status != 404:
                break
        if res.status == 404 or action == 'list_parts':
            return res

    bucket = bucket or get_submission_bucket()
    res = bucket.connection.make_request(
        method, bucket=bucket.name, key=key_name,
        data=data, query_args=args, headers=headers
    )
    return res
