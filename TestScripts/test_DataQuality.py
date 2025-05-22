import pandas as pd
from sqlalchemy import create_engine
# import cx_Oracle
import pytest
import psycopg2

from CommonUtilities.utils import verify_expected_from_files_to_actual_from_db, \
    verify_expected_from_db_to_actual_from_db, check_file_exists, check_file_size_for_data, check_duplicates_rows
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


class TestDataQuality:
    def test_DataQuality_supplier_data_File_availabilty_check(self):
        logger.info("Test cases for supplier_data- file availability check has started ....")
        try:
            assert check_file_exists(
                "TestData/supplier_data.json") == True, "Supplier_data.json file does not exist in the path"
        except Exception as e:
            logger.error(f" error occurred during test {e}")
            pytest.fail("error occurred during test")

    def test_DataQuality_supplier_data_File_zero_byte_check(self):
        logger.info("Test cases for supplier_data- zero byte size check has started ....")
        try:
            assert check_file_size_for_data(
                "TestData/supplier_data.json") == True, "Supplier_data.json file has no data"
        except Exception as e:
            logger.error(f" error occurred during test {e}")
            pytest.fail("error occurred during test")

    def test_DataQuality_supplier_data_File_duplicate_row_check(self):
        logger.info("Test cases for supplier_data- duplicate row check has started ....")
        try:
            assert check_duplicates_rows("TestData/supplier_data.json",
                                         "json") == True, "Supplier_data.json file has duplicate rows"
        except Exception as e:
            logger.error(f" error occurred during test {e}")
            pytest.fail("error occurred during test")

    def test_DataQuality_supplier_data_File_missing_data_or_null_vaule_check(self):
        logger.info("Test cases for supplier_data- null value heck has started ....")
        try:
            assert check_duplicates_rows("TestData/supplier_data.json",
                                         "json") == True, "Supplier_data.json file has some null values"
        except Exception as e:
            logger.error(f" error occurred during test {e}")
            pytest.fail("error occurred during test")
