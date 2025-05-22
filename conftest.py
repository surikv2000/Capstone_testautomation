import pandas as pd
from sqlalchemy import create_engine
# import cx_Oracle
import pytest
import psycopg2
from Configuration.ETLConfigs import *
# from Utilities.ReadFiles import *

import logging

# Logging configuration
logging.basicConfig(
    filename="Logs/ETLLogs.log",
    filemode="w",  # a for append the log file and w for overwrite
    format='%(asctime)s-%(levelname)s-%(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


@pytest.fixture()
def connect_to_pg_db():
    logger.info("Postgres connection is being established...")
    pg_engine = create_engine(
        f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}").connect()
    logger.info("Postgres connection has been established...")
    yield pg_engine
    pg_engine.close()
    logger.info("Postgres connection has been closed..")


@pytest.fixture()
def connect_to_mysql_db():
    logger.info("mysql connection is being established...")
    mysql_engine = create_engine(
        f"mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{msql_port}/{mysql_database}").connect()
    logger.info("mysql connection has been established...")
    yield mysql_engine
    mysql_engine.close()
    logger.info("mysql connection has been closed..")


@pytest.fixture()
def print_message():
    logger.info("This is pre test fixture...")
    yield
    logger.info("This is post test fixture...")
