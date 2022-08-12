# DAG
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Essentials
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Sourcing
import riotwatcher as rt
from riotwatcher import LolWatcher, ApiError
import requests
import urllib.request
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import itertools
import concurrent.futures

# formats
import json
import pyarrow.parquet as pq

# time
import datetime
import pendulum
import time

# Others - NEW ADDED (spark)
import psycopg2
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/project/spark-3.2.1-bin-hadoop3.2"
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

# ML ml_training
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import RandomizedSearchCV, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score
import joblib

# importing self-made functions
from custom_package import extract_funcs, transform_funcs, load_funcs

# extract
def extract():
    focused_streamers = extract_funcs.get_streamers('all')
    player_deets = extract_funcs.player_details(focused_streamers)
    last_matches = extract_funcs.multi_processing(focused_streamers)
    puuid_frame = extract_funcs.get_puuid(last_matches)
    warding_df, kd_df, scores10_df, match_info_df, player_match_df, chosen_champion_df = extract_funcs.match_statistics(puuid_frame, 1)
    champions_df = extract_funcs.champions_dataframe()
    focused_streamers.to_csv('/project/experimentation/etl_files_exp/focused_streamers.csv')
    player_deets.to_csv('/project/experimentation/etl_files_exp/player_deets.csv')
    last_matches.to_csv('/project/experimentation/etl_files_exp/last_match_account.csv')
    puuid_frame.to_csv('/project/experimentation/etl_files_exp/summoner_info.csv')
    warding_df.to_csv('/project/experimentation/etl_files_exp/warding_df.csv')
    kd_df.to_csv('/project/experimentation/etl_files_exp/kd_df.csv')
    scores10_df.to_csv('/project/experimentation/etl_files_exp/scores10_df.csv')
    match_info_df.to_csv('/project/experimentation/etl_files_exp/match_info_df.csv')
    player_match_df.to_csv('/project/experimentation/etl_files_exp/player_match_outcome.csv')
    chosen_champion_df.to_csv('/project/experimentation/etl_files_exp/chosen_champion_df.csv')
    champions_df.to_csv('/project/experimentation/etl_files_exp/champions_df.csv')

# transform
def transform():
    sp = transform_funcs.start_spark()
    
    streamers1_df_path = '/project/experimentation/etl_files_exp/focused_streamers.csv'
    streamers2_df_path = '/project/experimentation/etl_files_exp/player_deets.csv'
    last_match_df_path = '/project/experimentation/etl_files_exp/last_match_account.csv'
    summoner_info_df_path = '/project/experimentation/etl_files_exp/summoner_info.csv'
    warding_df_path = '/project/experimentation/etl_files_exp/warding_df.csv'
    kd_df_path = '/project/experimentation/etl_files_exp/kd_df.csv'
    scores10_df_path = '/project/experimentation/etl_files_exp/scores10_df.csv'
    match_info_df_path = '/project/experimentation/etl_files_exp/match_info_df.csv'
    player_match_outcome_df_path = '/project/experimentation/etl_files_exp/player_match_outcome.csv'
    chosen_champion_df_path = '/project/experimentation/etl_files_exp/chosen_champion_df.csv'
    champions_df_path = '/project/experimentation/etl_files_exp/champions_df.csv'
    
    streamers1_df = transform_funcs.spark_read_csv(sp, streamers1_df_path)
    streamers2_df = transform_funcs.spark_read_csv(sp, streamers2_df_path)
    last_match_df = transform_funcs.spark_read_csv(sp, last_match_df_path)
    summoner_df = transform_funcs.spark_read_csv(sp, summoner_info_df_path)
    warding_df = transform_funcs.spark_read_csv(sp, warding_df_path)
    kd_df = transform_funcs.spark_read_csv(sp, kd_df_path)
    scores10_df = transform_funcs.spark_read_csv(sp, scores10_df_path)
    match_info_df = transform_funcs.spark_read_csv(sp, match_info_df_path)
    player_match_outcome_df = transform_funcs.spark_read_csv(sp, player_match_outcome_df_path)
    chosen_champions_df = transform_funcs.spark_read_csv(sp, chosen_champion_df_path)
    champions_df = transform_funcs.spark_read_csv(sp, champions_df_path)
    
    country_table = streamers2_df.select(streamers2_df["Birthplace"].alias('country_name'))
    role_table = streamers1_df.select(streamers1_df["roleNum"].alias('role_id') ,streamers1_df["role"].alias('role_name'))
    role_table = role_table.withColumn("role_id", role_table["role_id"].cast(IntegerType()))
    champion_table = champions_df.drop("_c0", "style") 
    champion_table = champion_table.withColumn("champion_id", champion_table["champion_id"].cast(IntegerType()))\
                        .withColumn("attack", champion_table["attack"].cast(IntegerType()))\
                        .withColumn("defense", champion_table["defense"].cast(IntegerType()))\
                        .withColumn("magic", champion_table["magic"].cast(IntegerType()))
    streamer_table_part1 = streamers1_df.select(streamers1_df["DT_RowId"],streamers1_df["plug"], 
                                            streamers1_df['roleNum'])
    streamer_table_part2 = streamers2_df.select(streamers2_df['DT_RowId'], streamers2_df['Name'], 
                                                streamers2_df['Birthplace'])
    streamer_table = streamer_table_part1.join(streamer_table_part2, ['DT_RowId'], how='outer')
    # rearranging the columns
    streamer_table = streamer_table.select(streamer_table['DT_RowId'].alias('streamer_id'), streamer_table['plug'].alias('streamer_name'), 
                                           streamer_table['Name'].alias('real_name'), streamer_table['Birthplace'].alias('birth_place'), 
                                           streamer_table['roleNum'].alias('role_id'))
    streamer_table = streamer_table.withColumn("streamer_id", streamer_table["streamer_id"].cast(IntegerType()))\
                        .withColumn("role_id", streamer_table["role_id"].cast(IntegerType())) 
    summoner_table1 = summoner_df.select(summoner_df['puuid'], summoner_df['account_name'])
    summoner_table2 = last_match_df.select(last_match_df['account_name'], last_match_df['DT_RowId'].alias('streamer_id'))
    summoner_table = summoner_table1.join(summoner_table2, ['account_name'], how='outer')
    
    # Rearrange columns
    summoner_table = summoner_table.select(summoner_table['puuid'], summoner_table['account_name'], summoner_table['streamer_id'])
    
    summoner_table = summoner_table.withColumn("streamer_id", summoner_table["streamer_id"].cast(IntegerType()))
    match_table = match_info_df
    match_table = match_table.withColumn("duration", F.ceil((F.col('duration')) / 60000)).drop('_c0')
    match_table = match_table.withColumn("duration", match_table["duration"].cast(IntegerType())) 
    outcome_table_columns = ['outcome_id', 'outcome_name']
    outcome_table_values = [(0, "loss"), (1, "win")]
    outcome_table = sp.createDataFrame(outcome_table_values).toDF(*outcome_table_columns)
    outcome_table = outcome_table.withColumn("outcome_id", outcome_table["outcome_id"].cast(IntegerType()))  
    performance_table1 = player_match_outcome_df.withColumn('outcome', 
                                                            F.when(player_match_outcome_df.outcome == "loss", 
                                                            F.regexp_replace(player_match_outcome_df.outcome, 'loss', '0'))\
                                                            .when(player_match_outcome_df.outcome == "win",
                                                            F.regexp_replace(player_match_outcome_df.outcome, 'win', '1')))\
                                                            .withColumnRenamed('outcome', 'outcome_id').drop("_c0")
    # wards
    performance_table2 = warding_df.filter(warding_df.timestamp < 600100)\
                        .groupBy(["match_id", "puuid"]).count()\
                        .withColumnRenamed('count', 'ward_count')
    # kills
    performance_table3 = kd_df.filter((kd_df.timestamp < 600100) & (kd_df.main == kd_df.killerId))\
                        .groupBy(["match_id", "puuid", "killerId"]).count()\
                        .withColumnRenamed('count', 'no_kills')\
                        .drop("killerId")
    # deaths
    performance_table4 = kd_df.filter((kd_df.timestamp < 600100) & (kd_df.main == kd_df.victimId))\
                        .groupBy(["match_id", "puuid", "victimId"]).count()\
                        .withColumnRenamed('count', 'no_deaths')\
                        .drop("victimId") 
    # other scores
    performance_table5 = scores10_df.drop("_c0")
    # chosen champion (playable character)
    performance_table6 = chosen_champions_df.drop("_c0") 
    # # Final Table
    performance_table = performance_table6.join(performance_table3, ["match_id", "puuid"], how='outer')\
    .join(performance_table4, ["match_id", "puuid"], how='outer')\
    .join(performance_table5, ["match_id", "puuid"], how='outer')\
    .join(performance_table2, ["match_id", "puuid"], how='outer')\
    .join(performance_table1, ["match_id", "puuid"], how='outer')
    performance_table = performance_table.withColumn("champion_id", performance_table["champion_id"].cast(IntegerType()))\
                        .withColumn("no_kills", performance_table["no_kills"].cast(IntegerType()))\
                        .withColumn("no_deaths", performance_table["no_deaths"].cast(IntegerType()))\
                        .withColumn("gold_at_10", performance_table["gold_at_10"].cast(IntegerType()))\
                        .withColumn("minions_at_10", performance_table["minions_at_10"].cast(IntegerType()))\
                        .withColumn("xp_at_10", performance_table["xp_at_10"].cast(IntegerType()))\
                        .withColumn("ward_count", performance_table["ward_count"].cast(IntegerType()))\
                        .withColumn("outcome_id", performance_table["outcome_id"].cast(IntegerType())).fillna(0) 
    path = '/project/experimentation/etl_files_exp/'
    transform_funcs.save_pyspark_df(country_table, path, "country_table")
    transform_funcs.save_pyspark_df(role_table, path, "role_table")
    transform_funcs.save_pyspark_df(champion_table, path, "champion_table")
    transform_funcs.save_pyspark_df(streamer_table, path, "streamer_table")
    transform_funcs.save_pyspark_df(summoner_table, path, "summoner_table")
    transform_funcs.save_pyspark_df(match_table, path, "match_table")
    transform_funcs.save_pyspark_df(outcome_table, path, "outcome_table")
    transform_funcs.save_pyspark_df(performance_table, path, "performance_table")
    transform_funcs.close_spark(sp)

# load 
def load():
    conn = load_funcs.create_conn()
    cursor = conn.cursor()
    schema_exists = load_funcs.tables_exist(cursor)
    load_funcs.create_tables(schema_exists, cursor)
    sp = load_funcs.start_spark()
    
    country_table = load_funcs.spark_read_parquet(sp, "/project/experimentation/etl_files_exp/country_table.parquet")
    role_table = load_funcs.spark_read_parquet(sp, "/project/experimentation/etl_files_exp/role_table.parquet")
    champion_table = load_funcs.spark_read_parquet(sp, "/project/experimentation/etl_files_exp/champion_table.parquet")
    streamer_table = load_funcs.spark_read_parquet(sp, "/project/experimentation/etl_files_exp/streamer_table.parquet")
    summoner_table = load_funcs.spark_read_parquet(sp, "/project/experimentation/etl_files_exp/summoner_table.parquet")
    match_table = load_funcs.spark_read_parquet(sp, "/project/experimentation/etl_files_exp/match_table.parquet")
    outcome_table = load_funcs.spark_read_parquet(sp, "/project/experimentation/etl_files_exp/outcome_table.parquet")
    performance_table = load_funcs.spark_read_parquet(sp, "/project/experimentation/etl_files_exp/performance_table.parquet")
    
    country_tuples = load_funcs.frame_to_tuples(country_table)
    role_tuples = load_funcs.frame_to_tuples(role_table)
    champion_tuples = load_funcs.frame_to_tuples(champion_table)
    streamer_tuples = load_funcs.frame_to_tuples(streamer_table)
    summoner_tuples = load_funcs.frame_to_tuples(summoner_table)
    match_tuples = load_funcs.frame_to_tuples(match_table)
    outcome_tuples = load_funcs.frame_to_tuples(outcome_table)
    performance_tuples = load_funcs.frame_to_tuples(performance_table) 

    load_funcs.populate_db(cursor, country_tuples, role_tuples, champion_tuples, streamer_tuples, 
                    summoner_tuples, match_tuples, outcome_tuples, performance_tuples)
    country_table_name = "country"
    role_table_name = "role"
    champion_table_name = "champion"
    streamer_table_name = "streamer"
    summoner_table_name = "summoner"
    match_table_name = "match"
    outcome_table_name = "outcome"
    performance_table_name = "performance"
    
    schema_name = 'lol_games'
    country_psql = load_funcs.get_table_from_postgres(sp, cursor, schema_name, country_table_name)
    role_psql = load_funcs.get_table_from_postgres(sp, cursor, schema_name, role_table_name)
    champion_psql = load_funcs.get_table_from_postgres(sp, cursor, schema_name, champion_table_name)
    streamer_psql = load_funcs.get_table_from_postgres(sp, cursor, schema_name, streamer_table_name)
    summoner_psql = load_funcs.get_table_from_postgres(sp, cursor, schema_name, summoner_table_name)
    match_psql = load_funcs.get_table_from_postgres(sp, cursor, schema_name, match_table_name)
    outcome_psql = load_funcs.get_table_from_postgres(sp, cursor, schema_name, outcome_table_name)
    performance_psql = load_funcs.get_table_from_postgres(sp, cursor, schema_name, performance_table_name)
    
    ml_table = performance_psql.join(champion_psql, ['champion_id'], how='left').drop('champion_id', 'performance_id', 
                                                                                  'match_id', 'summoner_id', 'champion_name')
    ml_table.write.mode('overwrite').parquet('/project/experimentation/etl_files_exp/ml_table.parquet')
    
    load_funcs.commit_changes(conn)
    load_funcs.close_conn(conn)
    load_funcs.close_spark(sp)

def ml_training():
    ml_df = pd.read_parquet('/project/experimentation/etl_files_exp/ml_table.parquet')
    ssc = StandardScaler()
    logmodel = LogisticRegression(max_iter = 10000)
    ml_pipeline = Pipeline(steps=[("ssc", ssc), ("log", logmodel)])
    X = ml_df.drop('outcome_id', axis=1)
    y = ml_df['outcome_id']
    params_range = {
        "log__C": list(np.arange(0,5, 0.5)), 
        "log__solver": ['saga', 'sag', 'lbfgs', 'newton-cg'], 
    }
    rs = RandomizedSearchCV(ml_pipeline, params_range, n_iter =10, cv=3) # randomised search
    rs.fit(X, y)
    with open('/project/experimentation/model_artifacts/lm_model.joblib', 'wb') as f:
        joblib.dump(rs,f)
    
with DAG(
    'ETL_streamers_eSports',
    default_args={'retries': 0},
    description="ETL for streamers' games",
    schedule_interval='@hourly',
    start_date=pendulum.datetime(2022, 4, 20, tz="UTC"),
    catchup=False,
    tags=['streamers_lol_etl'],
) as dag:

    
    t1 = PythonOperator(
        task_id='Extract',
        python_callable=extract,
    )
    
    t2 = PythonOperator(
        task_id='Transform',
        python_callable=transform,
    )
    
    t3 = PythonOperator(
        task_id='Load',
        python_callable=load,
    )

    t4 = PythonOperator(
        task_id='ML_Pipeline_Half',
        python_callable=ml_training,
    )

    t1 >> t2 >> t3 >> t4






