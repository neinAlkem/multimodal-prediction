import pandas as pd
import os
import argparse
import logging
from catboost import CatBoostClassifier, Pool
from sklearn.metrics import accuracy_score, classification_report
from google.cloud import storage
from sklearn.preprocessing import StandardScaler

logging.basicConfig( format='%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.DEBUG)

def upload_to_gcs(local_path:str, gcs_path:str):
    if not gcs_path.startswith("gs://"):
        raise ValueError('GCS Path format was wrong')
    
    bucket_name = gcs_path.split('/')[2]
    blob_path = '/'.join(gcs_path.split('/')[3:])
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    if os.path.isdir(local_path):
        for root, _, files in os.walk(local_path):
            for file_name in files:
                local_file = os.path.join(root, file_name)
                relative_path = os.path.relpath(local_file, local_path)
                gcs_blob_path = os.path.join(blob_path, relative_path)
                blob = bucket.blob(gcs_blob_path)
                blob.upload_from_filename(local_file)
        logging.info(f"File {local_path} sucesffuly uploaded to {gcs_path}")
    else:
        blob = bucket.blob(blob_path)
        blob.upload_from_filename(local_path)
        logging.info(f"File {local_path} berhasil diunggah ke {gcs_path}")
    
def main(args):
        
    logging.info(f'Loading Dataset from GCS...')
    try:
        train_data = pd.read_csv(args.train_data)
        test_data = pd.read_csv(args.test_data)
    except Exception as e:
        logging.error(f'Error reading data from GCS :, {e}')
        raise
    logging.info('Success..')
    
    logging.info('Prepparing Data')
    target_column = 'engagement_level'
    x_train = train_data.drop(target_column, axis=1)
    y_train = train_data[target_column]
    x_test = test_data.drop(target_column, axis=1)
    y_test = test_data[target_column]
    
    scaler = StandardScaler()
    x_train_scaled = scaler.fit_transform(x_train)
    x_test_scaled = scaler.transform(x_test)
    
    logging.info('Creating Catboost Pool...')
    train_pool = Pool(
        data=x_train_scaled,
        label=y_train
    )
    test_pool = Pool(
        data=x_test_scaled,
        label=y_test
    )
    
    logging.info('Training Model..')
    model = CatBoostClassifier(
        iterations=args.iteration,
        learning_rate=args.learning_rate,
        depth=args.depth,
        l2_leaf_reg=args.l2_leaf_reg,
        eval_metric='Accuracy',      
        random_seed=42,
        verbose=100,                 
        early_stopping_rounds=50   
    )
    model.fit(train_pool, 
              eval_set=test_pool)
    
    logging.info('Evaluating Model...')
    y_pred = model.predict(x_test)
    accuracy = accuracy_score(y_test, y_pred)
    report = classification_report(y_test, y_pred)
    
    logging.info(f"Akurasi Model Final: {accuracy:.4f}")
    logging.info(f"Laporan Klasifikasi:\n{report}")
    
    logging.info('Saving Model Artefact...')
    model_filename = 'model.cbm'
    model.save_model(model_filename)
    
    logging.info("Uploading Model to GCS...")
    upload_to_gcs(model_filename, os.path.join(args.model_output_dir, "model.cbm"))
    logging.info("Success...")

if __name__ == '__main__':    
    parser = argparse.ArgumentParser()
    parser.add_argument('--train-data', type=str, required=True, help='GCS path to training data.')
    parser.add_argument('--test-data', type=str, required=True, help='GCS path to testing data.')
    parser.add_argument('--model-output-dir', type=str, required=True, help='GCS path to save trained data.')
    
    parser.add_argument('--iterations', type=int, default=300, help='Maximum iteration')
    parser.add_argument('--learning-rate', type=float, default=0.01, help='Learning rate update')
    parser.add_argument('--depth', type=int, default=4, help='Trees maximum depth')
    parser.add_argument('--l2-leaf-reg', type=float, default=3, help='Coefession Regulation L2.')

    args = parser.parse_args()
    main(args)
        
    