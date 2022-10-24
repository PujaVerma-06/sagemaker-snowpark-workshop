
import argparse
import os
import warnings
import datetime
import pandas as pd
import numpy as np
import io
import joblib

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder, LabelBinarizer, KBinsDiscretizer
from sklearn.preprocessing import PolynomialFeatures
from sklearn.compose import make_column_transformer
from sklearn.exceptions import DataConversionWarning
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer

from snowflake.snowpark import (
    Column,
    DataFrame,
    Session,
    Window
)
from snowflake.snowpark import functions as f
from snowflake.snowpark.types import IntegerType, StringType, StructType, DateType, StructField, MapType
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, when, to_date


warnings.filterwarnings(action="ignore", category=DataConversionWarning)



CONNECTION_PARAMETERS = {
"account": "gi02106.eu-west-2.aws",
"user": "pujaverma",
"password": "Itzme#123",
"role": "accountadmin",
"warehouse": "workshopwh",
"database": "workshopdb",
"schema": "workshopsch",
}
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    #parser.add_argument("--train-test-split-ratio", type=float, default=0.3)
    args, _ = parser.parse_known_args()

    print("Received arguments {}".format(args))
    
   
    snowflake_conn_session = Session.builder.configs(CONNECTION_PARAMETERS).create()
    
    snowflake_conn_session.sql("select current_warehouse(), current_database(), current_schema(), current_role()").show()
    
    table_query = """create table inference_output (
    predictions varchar
    )"""
    
    snowflake_conn_session.sql(table_query).show()
    
    copy_data = """copy into inference_output
    from 's3://snowflake-stage-area1/batch-transform-output/test_features.csv.out'
    credentials = (aws_key_id = 'AKIA332DNHNSXTI2O5DH' aws_secret_key = '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')
    file_format = (format_name='public.csv_file_format');"""
    
    snowflake_conn_session.sql(copy_data).show()
