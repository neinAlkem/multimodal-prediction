from pyspark.sql import SparkSession, types, DataFrame
from pyspark.sql import functions as F
import logging
from pyspark.ml.feature import StringIndexer
import sys

logging.basicConfig( format='%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=logging.DEBUG)

def data_prep(spark:SparkSession, data_gcs:str) -> DataFrame :
    
    logging.info(f"Starting Data Preparation Process...")
    logging.info(f"Path info: {data_gcs}")

    logging.info(f"Loading Dataset from GCS..")
    try:
        df = spark.read.csv(data_gcs,  
                                    header=True,
                                    inferSchema=True)
    except Exception as e:
        logging.error(f'Error reading data from GCS : {e}')
        raise
        
    df = df.na.replace(['Engaged','Not Engaged'], 
                       ['Highly Engaged','Engaged'], 'engagement_level')
    
    categorical_columns = ['room','engagement_level']
    for col in categorical_columns:
        indexer = StringIndexer(inputCol=col, outputCol=f'{col}_index')
        df = indexer.fit(df).transform(df)
    
    train_data, test_data = df.randomSplit([0.8, 0.2], seed=123)
    
    return train_data, test_data

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: script.py <project_id> <fusion_data_gcs_path> <train_data_gcs_path> <test_data_gcs_path>")
        sys.exit(-1)
        
    project_id_env = sys.argv[1]
    fusion_data_gcs_path = sys.argv[2]
    train_data_gcs_path = sys.argv[3]
    test_data_gcs_path = sys.argv[4]
    
    spark = SparkSession.builder.appName('data_pipeline').getOrCreate()

    logging.info(f'Spark Session Started. Using Project: {project_id_env}')
    logging.info(f'Fusion Data Dir: {fusion_data_gcs_path}')
    logging.info(f'Training Data Dir: {train_data_gcs_path}')
    logging.info(f'Testing Dir: {test_data_gcs_path}')
    
    train_data, test_data = data_prep(spark, 
                   fusion_data_gcs_path)
    
    logging.info(f"Saving Training Data To: {train_data_gcs_path}")
    train_data.write.mode("overwrite").option("header", "true").csv(train_data_gcs_path)
    logging.info("Training Data saved successfully to GCS as CSV.")
    
    logging.info(f"Saving Testing Data To: {test_data_gcs_path}")
    test_data.write.mode("overwrite").option("header", "true").csv(train_data_gcs_path)
    logging.info("Testing Data saved successfully to GCS as CSV.")


    
    
    