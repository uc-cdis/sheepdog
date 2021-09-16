#!/usr/bin/env python
"""
Script to set up report database
"""

import argparse
from sqlalchemy import create_engine
from gdcdatamodel.models.submission import Base


def setup(host, port, user, password, database, use_ssl=False):
    connect_args = {}
    if use_ssl:
        connect_args["sslmode"] = "require"

    engine = create_engine(
        "postgres://{user}:{password}@{host}:{port}/{database}".format(
            user=user, host=host, port=port, password=password, database=database
        ),
        connect_args=connect_args,
    )
    Base.metadata.create_all(engine)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--host", type=str, action="store", default="localhost", help="psql-server host"
    )
    parser.add_argument(
        "--port", type=str, action="store", default="5432", help="psql-server port"
    )
    parser.add_argument(
        "--user", type=str, action="store", default="test", help="psql test user"
    )
    parser.add_argument(
        "--password",
        type=str,
        action="store",
        default="test",
        help="psql test password",
    )
    parser.add_argument(
        "--database",
        type=str,
        action="store",
        default="sheepdog_automated_test",
        help="psql test database",
    )
    parser.add_argument(
        "--use-ssl", type=bool, action="store", default=False, help="Use Psql SSL"
    )

    args = parser.parse_args()
    setup(
        args.host,
        args.port,
        args.user,
        args.password,
        args.database,
        use_ssl=args.use_ssl,
    )
