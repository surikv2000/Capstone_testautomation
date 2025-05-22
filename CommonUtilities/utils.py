import pandas as pd
from sqlalchemy import create_engine
# import cx_Oracle
import pytest
import os
# from Utilities.Utils import *
import logging
from Configuration.ETLConfigs import *

# Logging configuration
logging.basicConfig(
    filename="Logs/ETLLogs.log",
    filemode="w",  # a  for append the log file and w for overwrite
    format='%(asctime)s-%(levelname)s-%(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

mysql_engine = create_engine(f"mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{msql_port}/{mysql_database}")
pg_engine = create_engine(f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}")


def verify_expected_from_files_to_actual_from_db(file_path, file_type, query_actual, db_engine):
    try:
        if file_type == "csv":
            df_expected = pd.read_csv(file_path)
        elif file_type == "json":
            df_expected = pd.read_json(file_path)
        elif file_type == "xml":
            df_expected = pd.read_xml(file_path, xpath="./item")
        else:
            raise ValueError(f"unsupported file type passed {file_type}")
        df_actual = pd.read_sql(query_actual, db_engine)
        assert df_actual.equals(df_expected), "data does not match between expected and actual"
    except Exception as e:
        logger.error(f"data extraction from sales did not happen correctly {e}")


def verify_expected_from_db_to_actual_from_db(query_expected, db_engine_expected, query_actual, db_engine_actual):
    try:
        df_expected = pd.read_sql(query_expected, db_engine_expected).astype(str)
        logger.info(f"The expected data is :{df_expected}")
        df_actual = pd.read_sql(query_actual, db_engine_actual).astype(str)
        logger.info(f"The actual data is: {df_actual}")
        assert df_actual.equals(df_expected), "data does not match between expected and actual"

    except Exception as e:
        logger.error(f"data does not match between expected and actual{e}")
        pytest.fail("data does not match between expected and actual")


# Data Quality related functions

# utility for checking file exists

def check_file_exists(file_path):
    try:
        if os.path.isfile(file_path):
            return True
        else:
            return False
    except Exception as e:
        logger.error(f"File {file_path} does not exists {e}")


# utility for checking if file data exists

def check_file_size_for_data(file_path):
    try:
        if os.path.getsize(file_path) != 0:
            return True
        else:
            return False
    except Exception as e:
        logger.error(f"File {file_path} does not have data {e}")


def check_duplicates_rows(file_type, file_path):
    try:
        if file_type == "csv":
            df = pd.read_csv(file_path)
        elif file_type == "json":
            df = pd.read_json(file_path)
        elif file_type == "xml":
            df = pd.read_xml(file_path, xpath=".//item")
        else:
            raise ValueError(f"unsupported file type passed {file_type}")
        logger.info(f"The data is: {df}")
        if df.duplicated().any():
            return False
        else:
            return True
    except Exception as e:
        logger.error(f"error while reading the file {e}")


def check_for_null_values(file_type, file_path):
    try:
        if file_type == "csv":
            df = pd.read_csv(file_path)
        elif file_type == "json":
            df = pd.read_json(file_path)
        elif file_type == "xml":
            df = pd.read_xml(file_path, xpath=".//item")
        else:
            raise ValueError(f"unsupported file type {file_type}")
        logger.info(f"The data is: {df}")
        if df.isnull.any():
            return False
        else:
            return True
    except Exception as e:
        logger.error(f"error while reading the file {e}")
