

import argparse
import os
import warnings

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder, LabelBinarizer, KBinsDiscretizer
from sklearn.preprocessing import PolynomialFeatures
from sklearn.compose import make_column_transformer

from sklearn.exceptions import DataConversionWarning

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


columns = [
    "age",
    "class_of_worker",
    "education",
    "major_industry_code",
    "capital_gains",
    "capital_losses",
    "dividends_from_stocks",
    "num_persons_worked_for_employer",
    "income",
]
class_labels = [0, 1]

connection_parameters = {
"account": "",
"user": "",
"password": "",
"role": "",
"warehouse": "",
"database": "",
"schema": "",
}


def print_shape(df):
    print('*****IN print_shape df')
    negative_examples, positive_examples = np.bincount(df["income"])
    print(
        "Data shape: {}, {} positive examples, {} negative examples".format(
            df.shape, positive_examples, negative_examples
        )
    )
    

def transformation_pipeline():
    print('INSIDE TRANSFORMSTION PIPELINE')
    session = Session.builder.configs(CONNECTION_PARAMETERS).create()
    
    session.sql("select current_warehouse(), current_database(), current_schema()").show()
    
    df_train = session.table('SAGEMAKER_TABLE')

    df_train_pd = df_train.to_pandas()
    
    print("df_train_pd: ",df_train_pd.columns)
    return df_train_pd


if __name__ == "__main__":
    print('**** IN MAIN')
    parser = argparse.ArgumentParser()
    parser.add_argument("--train-test-split-ratio", type=float, default=0.3)
    args, _ = parser.parse_known_args()

    print("Received arguments {}".format(args))

#     input_data_path = os.path.join("/opt/ml/processing/input", "census-income.csv")

#     print("Reading input data from {}".format(input_data_path))
#     df = pd.read_csv(input_data_path)
    df = transformation_pipeline()
    pd.set_option('max_columns', None)
    print('******read snowflake table df', df.head(n=5))
    # df = pd.DataFrame(data=df, columns=columns)
    # print('******read_csv df', df.show(10))
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    #df.replace(class_labels, [0, 1], inplace=True)

    negative_examples, positive_examples = np.bincount(df["income"])
    print(
        "Data after cleaning: {}, {} positive examples, {} negative examples".format(
            df.shape, positive_examples, negative_examples
        )
    )

    split_ratio = args.train_test_split_ratio
    print("Splitting data into train and test sets with ratio {}".format(split_ratio))
    X_train, X_test, y_train, y_test = train_test_split(
        df.drop("income", axis=1), df["income"], test_size=split_ratio, random_state=0
    )

    preprocess = make_column_transformer(
        (
            KBinsDiscretizer(encode="onehot-dense", n_bins=10),
            ["age", "num_persons_worked_for_employer"],
        ),
        (StandardScaler(), ["capital_gains", "capital_losses", "dividends_from_stocks"]),
        (OneHotEncoder(sparse=False), ["education", "major_industry_code", "class_of_worker"]),
    )
    print("Running preprocessing and feature engineering transformations")
    train_features = preprocess.fit_transform(X_train)
    test_features = preprocess.transform(X_test)

    print("Train data shape after preprocessing: {}".format(train_features.shape))
    print("Test data shape after preprocessing: {}".format(test_features.shape))

    train_features_output_path = os.path.join("/opt/ml/processing/train", "train_features.csv")
    train_labels_output_path = os.path.join("/opt/ml/processing/train", "train_labels.csv")

    test_features_output_path = os.path.join("/opt/ml/processing/test", "test_features.csv")
    test_labels_output_path = os.path.join("/opt/ml/processing/test", "test_labels.csv")

    print("Saving training features to {}".format(train_features_output_path))
    pd.DataFrame(train_features).to_csv(train_features_output_path, header=False, index=False)

    print("Saving test features to {}".format(test_features_output_path))
    pd.DataFrame(test_features).to_csv(test_features_output_path, header=False, index=False)

    print("Saving training labels to {}".format(train_labels_output_path))
    y_train.to_csv(train_labels_output_path, header=False, index=False)

    print("Saving test labels to {}".format(test_labels_output_path))
    y_test.to_csv(test_labels_output_path, header=False, index=False)
