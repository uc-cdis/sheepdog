#!/usr/bin/env python
"""
Script to set up notifcations table
"""

from sqlalchemy import create_engine
from gdcdatamodel.models.notifications import Base


def setup(host, user, password, database):
    engine = create_engine(
        "postgres://{user}:{password}@{host}/{database}".format(
            user=user, host=host, password=password, database=database))
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
