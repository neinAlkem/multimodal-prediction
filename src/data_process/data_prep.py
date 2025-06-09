from pyspark.sql import SparkSession, types, DataFrame
from pyspark.sql import functions as F
import logging
from pyspark.ml.feature import StringIndexer
import sys
import argparse
from typing import Tuple

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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Preparation Spark Job")
    parser.add_argument('--input-path', type=str, required=True, help='GCS path of the fused data.')
    parser.add_argument('--train-output-path', type=str, required=True, help='GCS path to save the training data.')
    parser.add_argument('--test-output-path', type=str, required=True, help='GCS path to save the testing data.')

    args = parser.parse_args()
    
    spark = SparkSession.builder \
        .appName("DataFusionJob") \
        .master("local[*]") \
        .getOrCreate()
     
    logging.info(f'Fusion Data Dir: {args.input_path}')
    logging.info(f'Training Data Dir: {args.train_otuput_path}')
    logging.info(f'Testing Dir: {args.test_output_path}')
    
    try:
        train_data, test_data = data_prep(spark, 
                       args.input_path)
        
        # --------------------------- Saving Training Split -------------------------- #
        logging.info(f"Saving Training Data To: {args.train_otuput_path}")
        train_data.write.mode("overwrite").format("csv").option("header", "true").save(args.train_otuput_path)
        logging.info("Training Data saved successfully to GCS as CSV.")

        # --------------------------- Saving Testing Split --------------------------- #
        logging.info(f"Saving Testing Data To: {args.test_output_path}")
        train_data.write.mode("overwrite").format("csv").option("header", "true").save(args.test_output_path)
        logging.info("Testing Data saved successfully to GCS as CSV.")
        
    except Exception as e:
        logging.error(f'Error during data preparation process: {e}')
    finally:
        spark.stop()


    
    
    