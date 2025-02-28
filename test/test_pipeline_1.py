import numpy as np
from datetime import date
import pytest
from snowflake.snowpark import Session
from connector import connection_parameters


class Testing:     
    session = Session.builder.configs(connection_parameters).create()

    def count_weekdays(self,start_date='2020-02-23', end_date=date.today()):
        return np.busday_count(start_date, end_date).item()

    def test_method(self):
        assert self.count_weekdays() == self.session.sql("SELECT COUNT(*)  as CNT FROM FRED_DB.FRED_HARMONIZED.FRED_HARMONIZED_TABLE ").collect()[0][0]

   