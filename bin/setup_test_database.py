# -*- coding: utf-8 -*-
"""
bin.setup_test_database
----------------------------------

Setup test database as required for testing
"""

from setup_transactionlogs import setup as create_transaction_logs_table

import argparse

from setup_psqlgraph import setup_database, create_tables, create_indexes


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
        "--root_user",
        type=str,
        action="store",
        default="postgres",
        help="psql root (postgres) user name",
    )
    parser.add_argument(
        "--root_password",
        type=str,
        action="store",
        default="test",
        help="psql root (postgres) user password",
    )
    parser.add_argument(
        "--database",
        type=str,
        action="store",
        default="sheepdog_automated_test",
        help="psql test database",
    )
    parser.add_argument(
        "--no-drop", action="store_true", default=False, help="do not drop any data"
    )
    parser.add_argument(
        "--no-user", action="store_true", default=False, help="do not create user"
    )
    parser.add_argument(
        "--use-ssl", type=bool, action="store", default=False, help="Use Psql SSL"
    )

    args = parser.parse_args()
    setup_database(
        user=args.user,
        password=args.password,
        database=args.database,
        root_user=args.root_user,
        host=args.host,
        port=args.port,
        root_password=args.root_password,
        no_drop=args.no_drop,
        no_user=args.no_user,
        use_ssl=args.use_ssl,
    )
    create_tables(
        args.host,
        args.port,
        args.user,
        args.password,
        args.database,
        use_ssl=args.use_ssl,
    )
    create_indexes(
        args.host,
        args.port,
        args.user,
        args.password,
        args.database,
        use_ssl=args.use_ssl,
    )
    create_transaction_logs_table(
        args.host,
        args.port,
        args.user,
        args.password,
        args.database,
        use_ssl=args.use_ssl,
    )
