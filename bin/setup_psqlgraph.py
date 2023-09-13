import argparse

import logging
from sqlalchemy import create_engine

from gdcdatamodel.models import *
from psqlgraph import create_all, Node, Edge


def try_drop_test_data(  # nosec
    user,
    database="postgres",
    root_user="postgres",
    host="",
    port="5432",
    root_password="",
    default_database="postgres",
    use_ssl=False,
):
    print("Dropping old test data")
    connect_str = _get_connection_string(
        user=root_user,
        password=root_password,
        host=host,
        port=port,
        database=default_database,
    )

    # added in for Postgresql SSL testing.
    connect_args = {}
    if use_ssl:
        connect_args["sslmode"] = "require"

    engine = create_engine(connect_str, connect_args=connect_args)

    conn = engine.connect()
    conn.execute("commit")

    try:
        create_stmt = 'DROP DATABASE "{database}"'.format(database=database)
        conn.execute(create_stmt)
    except Exception as msg:
        logging.warning("Unable to drop test data:" + str(msg))

    conn.close()


def _get_connection_string(user, password, host, port, database):
    connect_str = "postgres://{user}@{host}:{port}/{database}".format(
        user=user, host=host, port=port, database=database
    )
    if password:
        connect_str = "postgres://{user}:{password}@{host}:{port}/{database}".format(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database,
        )
    return connect_str


def setup_database(  # nosec
    user,
    password,
    database,
    root_user="postgres",
    host="",
    port="5432",
    no_drop=False,
    no_user=False,
    root_password="",
    default_database="postgres",
    use_ssl=False,
):
    """
    setup the user and database
    """
    print("Setting up test database")

    if not no_drop:
        try_drop_test_data(
            user=user,
            database=database,
            root_user=root_user,
            host=host,
            port=port,
            root_password=root_password,
            default_database=default_database,
            use_ssl=use_ssl,
        )

    connect_str = _get_connection_string(
        user=root_user,
        password=root_password,
        host=host,
        port=port,
        database=default_database,
    )

    # added in for Postgresql SSL testing.
    connect_args = {}
    if use_ssl:
        connect_args["sslmode"] = "require"

    engine = create_engine(connect_str, connect_args=connect_args)
    conn = engine.connect()
    conn.execute("commit")

    # Use default db connection to set up schema
    create_stmt = 'CREATE DATABASE "{database}"'.format(database=database)
    try:
        conn.execute(create_stmt)
    except Exception as msg:
        logging.warning("Unable to create database: {}".format(msg))

    if not no_user:
        try:
            user_no_host = user if "@" not in user else user.split("@")[0]
            user_stmt = "CREATE USER {user} WITH PASSWORD '{password}'".format(
                user=user_no_host, password=password
            )
            conn.execute(user_stmt)
        except Exception as msg:
            logging.warning("Unable to add user:" + str(msg))
        # User may already exist - GRANT privs on new db
        try:
            perm_stmt = (
                "GRANT ALL PRIVILEGES ON DATABASE {database} to {user}"
                "".format(database=database, user=user_no_host)
            )
            conn.execute(perm_stmt)
            conn.execute("commit")
        except Exception as msg:
            logging.warning("Unable to GRANT privs to user:" + str(msg))

    # PostgreSQL 15 revokes the previously defaulted CREATE permission
    # from all users
    # except a database owner from the public (or default) schema.
    # This is required for db setup for testing, so grant
    # that permission to the user as well.
    try:
        perm_stmt = "GRANT CREATE ON SCHEMA public TO {user}".format(user=user)
        conn.execute(perm_stmt)
        conn.execute("commit")

        perm_stmt = "GRANT CREATE ON SCHEMA public TO {root_user}".format(
            user=root_user
        )
        conn.execute(perm_stmt)
        conn.execute("commit")
    except Exception as msg:
        logging.warning("Unable to GRANT privs to users:" + str(msg))

    conn.close()


def create_tables(host, port, user, password, database, use_ssl=False):
    """
    create a table
    """
    print("Creating tables in test database")

    # added for Postgresql SSL
    connect_args = {}
    if use_ssl:
        connect_args["sslmode"] = "require"

    engine = create_engine(
        _get_connection_string(
            user=user, password=password, host=host, port=port, database=database
        ),
        connect_args=connect_args,
    )
    create_all(engine)
    versioned_nodes.Base.metadata.create_all(engine)


def create_indexes(host, port, user, password, database, use_ssl=False):
    print("Creating indexes")

    # added for Postgresql SSL
    connect_args = {}
    if use_ssl:
        connect_args["sslmode"] = "require"

    engine = create_engine(
        _get_connection_string(
            user=user, password=password, host=host, port=port, database=database
        ),
        connect_args=connect_args,
    )
    index = lambda t, c: ["CREATE INDEX ON {} ({})".format(t, x) for x in c]
    for scls in Node.get_subclasses():
        tablename = scls.__tablename__
        list(map(engine.execute, index(tablename, ["node_id"])))
        list(
            map(
                engine.execute,
                [
                    "CREATE INDEX ON {} USING gin (_sysan)".format(tablename),
                    "CREATE INDEX ON {} USING gin (_props)".format(tablename),
                    "CREATE INDEX ON {} USING gin (_sysan, _props)".format(tablename),
                ],
            )
        )
    for scls in Edge.get_subclasses():
        list(
            map(
                engine.execute,
                index(scls.__tablename__, ["src_id", "dst_id", "dst_id, src_id"]),
            )
        )


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
        "--default-database",
        type=str,
        action="store",
        default="postgres",
        help="psql test database for root user",
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
        args.user,
        args.password,
        args.database,
        port=args.port,
        no_drop=args.no_drop,
        no_user=args.no_user,
        default_database=args.default_database,
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
