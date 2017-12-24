# -*- coding: utf-8 -*-
"""
bin.setup_test_database
----------------------------------

Setup test database as required for testing
"""

from setup_transactionlogs import setup as create_transaction_logs_table
from setup_notifications import setup as create_notifications_table

import argparse

from setup_psqlgraph import (
    setup_database,
    create_tables,
    create_indexes,
)


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
    parser.add_argument("--no-drop", action="store_true",
                        default=False, help="do not drop any data")
    parser.add_argument("--no-user", action="store_true",
                        default=False, help="do not create user")

    args = parser.parse_args()
    setup_database(args.user, args.password, args.database,
                   no_drop=args.no_drop, no_user=args.no_user)
    create_tables(args.host, args.user, args.password, args.database)
    create_indexes(args.host, args.user, args.password, args.database)
    create_transaction_logs_table(args.host, args.user, args.password, args.database)
    create_notifications_table(args.host, args.user, args.password, args.database)
