#!/usr/bin/env python
"""
Script to set up report database
"""

import argparse
from sqlalchemy import create_engine
from gdcdatamodel.models.submission import Base


def setup(host, user, password, database):
    engine = create_engine(
        "postgres://{user}:{password}@{host}/{database}".format(
            user=user, host=host, password=password, database=database))
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, action="store",
                        default='localhost', help="psql-server host")
    parser.add_argument("--user", type=str, action="store",
                        default='test', help="psql test user")
    parser.add_argument("--password", type=str, action="store",
                        default='test', help="psql test password")
    parser.add_argument("--database", type=str, action="store",
                        default='automated_test', help="psql test database")

    args = parser.parse_args()
    setup(args.host, args.user, args.password, args.database)
