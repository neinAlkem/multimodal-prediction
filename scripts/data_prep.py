from pyspark.sql import SparkSession, types, DataFrame
from pyspark.sql import functions as F
import logging
from pyspark.ml.feature import StringIndexer
import sys
import os
import io
from dotenv import load_dotenv
from typing import Tuple

load_dotenv()

logging.basicConfig( format='%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=logging.DEBUG)

def data_prep(spark:SparkSession, data_gcs:str) -> Tuple[DataFrame, DataFrame]:
    
    logging.info(f"Starting Data Preparation Process...")
    logging.info(f"Path info: {data_gcs}")

# ---------------------------------------------------------------------------- #
#                                   Load Data                                  #
# ---------------------------------------------------------------------------- #
    logging.info(f"Loading Dataset from GCS..")
    try:
        df = spark.read.csv(data_gcs,  
                                    header=True,
                                    inferSchema=True)
    except Exception as e:
        logging.error(f'Error reading data from GCS : {e}')
        raise
        
# ---------------------------------------------------------------------------- #
#                       Label Encode Categorical Columns                       #
# ---------------------------------------------------------------------------- #
    df = df.na.replace(['Engaged','Not Engaged'], 
                       ['Highly Engaged','Engaged'], 'engagement_level')
    
    categorical_columns = ['room','engagement_level']
    for col in categorical_columns:
        indexer = StringIndexer(inputCol=col, outputCol=f'{col}_index')
        df = indexer.fit(df).transform(df)

# ---------------------------------------------------------------------------- #
#                     Splitting Training and Testing Dataset                    #
# ---------------------------------------------------------------------------- #
    train_data, test_data = df.randomSplit([0.8, 0.2], seed=123)
    
    logging.info(f'Training data row: {train_data.count()}')
    logging.info(f'Training data row: {test_data.count()}')
    return train_data, test_data

import os
import logging
import argparse
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("project_id", help="Project id to authorize")
    parser.add_argument("fusion_data_gcs_path", help="GCS path to fusion data")
    parser.add_argument("train_data_gcs_path", help="GCS path where training data CSV will be saved")
    parser.add_argument("test_data_gcs_path", help="GCS path where testing data CSV will be saved")

    args = parser.parse_args()

    spark = SparkSession \
        .builder \
        .master('spark://spark-masters:7077') \
        .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE") \
        .config("spark.hadoop.fs.gs.project.id", args.project_id) \
        .appName("project_spark") \
        .getOrCreate()

    logging.info(f'Fusion Data Dir: {args.fusion_data_gcs_path}')
    logging.info(f'Training Data Dir: {args.train_data_gcs_path}')
    logging.info(f'Testing Dir: {args.test_data_gcs_path}')

    try:
        train_data, test_data = data_prep(spark, args.fusion_data_gcs_path)

        df_train_data = train_data.toPandas()
        df_test_data = test_data.toPandas()

        logging.info(f"Saving Training Data To: {args.train_data_gcs_path}")
        df_train_data.to_csv(args.train_data_gcs_path,
                             index=False,
                             header=True,
                             mode='w')
        logging.info("Training Data saved successfully to GCS as CSV.")

        logging.info(f"Saving Testing Data To: {args.test_data_gcs_path}")
        df_test_data.to_csv(args.test_data_gcs_path,
                            index=False,
                            header=True,
                            mode='w')
        logging.info("Testing Data saved successfully to GCS as CSV.")
    except Exception as e:
        logging.error(f'Error during data preparation process: {e}')
    finally:
        spark.stop()

if __name__ == "__main__":
    main()



    
    
    