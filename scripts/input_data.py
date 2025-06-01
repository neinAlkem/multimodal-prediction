import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F

def process_survey(spark:SparkSession, survey_gcs:str, participat_info_gsc:str,output_gcs:str):
    print(f"Starting process...")
    print(f"Path info: {participat_info_gsc}")
    print(f"Path survey: {survey_gcs}")
    print(f"Output: {output_gcs}")
    
    try:
        df_student = spark.read.csv(os.path.join(participat_info_gsc, "student.csv"),
                                    header=True,
                                    inferSchema=True)
        df_class = spark.read.csv(os.path.join(participat_info_gsc, "class_table.csv"),
                                    header=True,
                                    inferSchema=True)
        df_survey = spark.read.csv(os.path.join(survey_gcs, "student_survey.csv"),
                                    header=True,
                                    inferSchema=True)
    except Exception as e:
        print(f'Error reading data from GCS : {e}')
        
    # Schema for student_info dataset
    student_info_schema = types.StructType([
         types.StructField('Pid', types.IntegerType(), True),
         types.StructField('Gender', types.StringType(), True),
         types.StructField('Age', types.IntegerType(), True),
         types.StructField('Form Room', types.StringType(), True),
         types.StructField('Math Room', types.StringType(), True),
         types.StructField('Language Room', types.StringType(), True),
         types.StructField('What is your general feeling in the classroom? - 1', types.IntegerType(), True),
         types.StructField("When I am engaged in class: - I usually don't feel too hot or too cold.", types.StringType(), True),
         types.StructField('When I am engaged in class: - I could get distracted when the room is too hot or too cold.', types.StringType(), True),
     ])
    for field in student_info_schema.fields:
         df_student = df_student.withColumn(field.name, F.col(f"`{field.name}`").cast(field.dataType))

    df_student = df_student.withColumnRenamed("What is your general feeling in the classroom? - 1", "General Feeling in Classroom")
    df_student = df_student.withColumnRenamed("When I am engaged in class: - I usually don't feel too hot or too cold.", "I usually don't feel too hot or cold in class (Engaged)")
    df_student = df_student.withColumnRenamed("When I am engaged in class: - I could get distracted when the room is too hot or too cold.", "Distracted by Temperature (Engaged)")

    class_schema = types.StructType([
         types.StructField('Class_id', types.IntegerType(), True),
         types.StructField('Room', types.StringType(), True),
         types.StructField('Start_time', types.TimestampType(), True),
         types.StructField('Finish_time', types.TimestampType(), True),
         types.StructField('Class_len', types.StringType(), True),
         types.StructField('Week', types.IntegerType(), True),
         types.StructField('Weekday', types.IntegerType(), True),
         types.StructField("Class_no", types.IntegerType(), True),
         types.StructField('Subject', types.StringType(), True),
         types.StructField('is_Form', types.StringType(), True),
     ])
    for field in class_schema.fields:
         df_class = df_class.withColumn(field.name, F.col(f"`{field.name}`").cast(field.dataType))

    survey_schema = types.StructType([
         types.StructField('Pid', types.IntegerType(), True),
         types.StructField('Week', types.IntegerType(), True),
         types.StructField('Weekday', types.IntegerType(), True),
         types.StructField('Time', types.TimestampType(), True),
         types.StructField('Thermal_sensation', types.IntegerType(), True),
         types.StructField('Thermal_preference', types.StringType(), True),
         types.StructField('Clothing', types.StringType(), True),
         types.StructField("Loc_x", types.IntegerType(), True),
         types.StructField('Loc_y', types.IntegerType(), True),
         types.StructField('Engage_1', types.StringType(), True),
         types.StructField('Engage_2', types.StringType(), True),
         types.StructField('Engage_3', types.StringType(), True),
         types.StructField('Engage_4', types.StringType(), True),
         types.StructField('Engage_5', types.StringType(), True),
         types.StructField('Arousal', types.IntegerType(), True),
         types.StructField('Valence', types.IntegerType(), True),
         types.StructField('Confidence_level', types.IntegerType(), True),
     ])
    for field in survey_schema.fields:
         df_survey = df_survey.withColumn(field.name, F.col(f"`{field.name}`").cast(field.dataType))
    
    student = df_student
    survey = df_survey
    table = df_class
    
    student = student.select('Pid', 'Gender', 'Age')
    rename_dict = {
        'Pid': 'student_id',
        'Gender': 'gender_code',
        'Age': 'age',
        'Form Room': 'form_room',
        'Math Room': 'math_room',
        'Language Room': 'language_room'
    }

    for old_name, new_name in rename_dict.items():
        if old_name in student.columns:
            student = student.withColumnRenamed(old_name, new_name)

    student = student.withColumn('gender_code', F.when(F.col('gender_code') == 'female',0).otherwise(1))
    
    survey = survey.withColumn(
    "Timestamp",
    F.from_unixtime((F.unix_timestamp("Time") / 300).cast("integer") * 300).cast("timestamp"))
    survey = survey.drop('Time')
    
    survey = survey.select('Pid','Week','Weekday','Timestamp','Engage_1', 'Engage_2','Engage_3','Engage_4','Engage_5')
    survey = survey.withColumn("time", F.date_format("timestamp", "HH:mm:ss"))
    survey = survey.drop('timestamp')
    survey = survey.withColumnRenamed('Pid', 'student_id')
    survey = survey.toDF(*[col.lower() for col in survey.columns])
    
    def engagement_score(col_name):
        return F.when(F.col(col_name) == "Strongly agree", 2)\
                    .when(F.col(col_name) == "Somewhat agree", 1)\
                    .when(F.col(col_name) == "Neither agree nor disagree", 0)\
                    .when(F.col(col_name) == "Somewhat disagree", -1)\
                    .when(F.col(col_name) == "Strongly disagree", -2)\
                    .otherwise(0)

    engage_cols_list = ['Engage_1', 'Engage_2', 'Engage_3', 'Engage_4', 'Engage_5']
    for i, engage_col in enumerate(engage_cols_list):
        survey = survey.withColumn(f"E{i+1}_score", engagement_score(engage_col))

    survey = survey.withColumn(
            "engagements_score",
            F.expr("E1_score + E2_score + E3_score + E4_score + E5_score")
        )
    
    cols_to_drop_engage_raw = engage_cols_list + [f"E{i+1}_score" for i in range(len(engage_cols_list))]
    survey = survey.drop(*cols_to_drop_engage_raw)
    
    survey = survey.withColumn("engagement_level",
    (F.col('engagements_score')/ 5))

    survey = survey.withColumn("engagement_level",
        F.when(F.col("engagements_score") >= 1, "Highly Engaged")
        .when(F.col("engagements_score") >= 0, "Engaged")
        .otherwise("Not Engaged")
    )
    survey = survey.drop('engagements_score')
    
    table = df_class.select('Room','Class_id','Weekday','Week','Start_time','Finish_time')
    
    table = table.withColumnsRenamed({'Weekday':'weekday_1', 'Week':'week_1', 'Room':'room','Class_id':'class_id'})

    table = table.withColumn('start',F.date_format('Start_time','HH:mm:ss')).drop('Start_time')
    table = table.withColumn('end',F.date_format('Finish_time','HH:mm:ss')).drop('Finish_time')
    
    df_joined = student.join(survey, on='student_id', how='left')
    
    df_combined = df_joined.join(table, on=[
    (df_joined['week'] == table['week_1']),
    (df_joined['weekday'] == table['weekday_1']),
    (df_joined['time'] >= table['start']),
    (df_joined['time'] <= table['end'])
    ], how='right')
    
    df_combined = df_combined.drop('week_1','weekday_1','start','end')
    return df_combined

def wearable_data(spark:SparkSession, wearable_gcs:str, output_gcs:start)
    print('Starting process wearabled data...')
    
    schema = types.StructType([
        types.StructField('value', types.FloatType(), True),
        types.StructField('Time', types.TimestampType(), True)
    ])

    hr = spark.read.schema(schema).csv(wearable_gcs + '/*/*/HR.csv')
    temp = spark.read.schema(schema).csv(wearable_gcs + '/*/*/TEMP.csv')
    eda = spark.read.schema(schema).csv(wearable_gcs + '/*/*/EDA.csv')
    ibi = spark.read.schema(schema).csv(wearable_gcs + '/*/*/IBI.csv')
    bvp = spark.read.schema(schema).csv(wearable_gcs + '/*/*/BVP.csv')
    
    def extract_id(df):
        df = df.withColumn('class_id', F.regexp_extract(F.input_file_name(),r'class_wearable_data/(\d+)/',1).cast('int'))
        df = df.withColumn('participant_id', F.regexp_extract(F.input_file_name(),r'class_wearable_data/\d+/(\d+)/',1).cast('int'))
        return df

    hr = extract_id(hr)
    temp = extract_id(temp)
    eda = extract_id(eda)
    ibi = extract_id(ibi)
    bvp = extract_id(bvp)
    
    def format_time(df):
        return df.withColumn('time_format',
                         F.date_format('Time','HH:mm:ss')).drop('Time')

    hr_time = format_time(hr)
    temp_time = format_time(temp)
    eda_time = format_time(eda)
    ibi_time = format_time(ibi)
    bvp_time = format_time(bvp)
    
    def agg_sensor(df, alias):
        return df.groupby('class_id', 'participant_id', F.window('time_format','5 minutes')
                        ).agg(F.mean('value').alias(alias))

    hr_agg = agg_sensor(hr_time, 'hr_mean')
    temp_agg = agg_sensor(temp_time, 'temp_mean')
    eda_agg = agg_sensor(eda_time, 'eda_mean')
    ibi_agg = agg_sensor(ibi_time, 'ibi_mean')
    bvp_agg = agg_sensor(bvp_time, 'bvp_mean')
    
    df_to_join = [hr_agg, temp_agg, eda_agg, ibi_agg, bvp_agg]
    df_join = reduce(lambda left,right:
        left.join(right, on=
                ['class_id',
                'participant_id',
                'window'], how='outer'),
        df_to_join)
    
    wearable_df = df_join.withColumn('time', F.col('window').start).withColumn('time_format', F.date_format('time', 'HH:mm:ss')) \
        .select('class_id', 'participant_id', 'time_format','hr_mean', 'temp_mean', 'eda_mean', 'ibi_mean', 'bvp_mean'
    ).orderBy('class_id', 'participant_id', 'time')

    return wearable_df
                
if __name__ == "__main__":
    project_id_env = os.getenv('PROJECT_ID', 'default-project-id') 

    spark = SparkSession \
        .builder \
        .master('spark://localhost:7077') \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1") \
        .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE") \
        .config("spark.hadoop.fs.gs.project.id", os.getenv('PROJECT_ID')) \
        .appName("participants_survey") \
        .getOrCreate()
        
    base_gcs_dir_main = "gs://project-abd/raw/" 
    participant_info_input_dir_main = os.path.join(base_gcs_dir_main, "participant_class_info/")
    survey_input_dir_main = os.path.join(base_gcs_dir_main, "survey/")
    wearable_gcs = 'gs://project-abd/raw/class_wearable_data/'

    final_output_gcs_path_main = "gs://project-abd/processed/participant_survey.csv"    
    print("Running main function...")
    # Panggil fungsi utama
    processed_df = process_survey(
        spark, 
        participant_info_input_dir_main, 
        survey_input_dir_main, 
        final_output_gcs_path_main
    )
    
    print("Menghentikan SparkSession.")
    spark.stop()    

