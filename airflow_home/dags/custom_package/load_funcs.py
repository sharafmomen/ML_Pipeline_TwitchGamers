
##### Loading

# Importing Packages
import os
os.environ["JAVA_HOME"] = "/project/jars_and_packages/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/project/jars_and_packages/spark-3.2.1-bin-hadoop3.2"
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType
import pyarrow.parquet as pq

import numpy as np # linear algebra
import pandas as pd 
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
import sqlalchemy

import io
from io import StringIO, BytesIO
import boto3
import psycopg2

# Spark functions for Load

# start spark
def start_spark():
    spark = SparkSession \
            .builder \
            .appName("PySpark App") \
            .getOrCreate()
    return spark

# close spark
def close_spark(spark_sesh):
    spark_sesh.stop()
    
# read parquet files
def spark_read_parquet(spark_session, path):
    df = spark_session.read.parquet(path)
    return df

# turn spark dataframe into list of tuples
def frame_to_tuples(df):
    l = []
    temp = df
    for i in temp.collect():
        l.append(tuple(i))
    return l
    
##### PSYCOPG2

# start psycopg2 connection:
def create_conn():
    conn = psycopg2.connect(
    database="postgres", 
        user='postgres', 
        password='qwerty123', 
        host="lolwinner.clmauzeuspbj.us-east-1.rds.amazonaws.com", 
        port= '5432'
    )
    return conn

# close psycopg2 connection:
def close_conn(conn):
    conn.commit()
    conn.close()
    
# check if tables exist - assuming the existence of one table means existence of all:
def tables_exist(cursor):
    cursor.execute("select * from information_schema.schemata where schema_name='lol_games'")
    return bool(cursor.rowcount)

# create schema and tables
def create_tables(schema_exists, cursor):
    if schema_exists == True:
        return "exists"   
    country = """
            CREATE TABLE IF NOT EXISTS lol_games.country (
            country_name varchar PRIMARY KEY
            )
            """
    role = """
            CREATE TABLE IF NOT EXISTS lol_games.role (
            role_id int PRIMARY KEY,
            role_name varchar
            )
            """
    champion = """
              CREATE TABLE IF NOT EXISTS lol_games.champion (
              champion_id int PRIMARY KEY,
              champion_name varchar,
              attack int,
              defense int,
              magic int
            )
            """    
    streamer = """
              CREATE TABLE IF NOT EXISTS lol_games.streamer (
              streamer_id int PRIMARY KEY,
              streamer_name varchar,
              real_name varchar,
              birth_country varchar references lol_games.country("country_name"),
              role_id int references lol_games.role("role_id")
            )
            """    
    summoner = """
              CREATE TABLE IF NOT EXISTS lol_games.summoner (
              puuid varchar PRIMARY KEY,
              account_name varchar,
              streamer_id int references lol_games.streamer("streamer_id")
            )
            """
    match = """
              CREATE TABLE IF NOT EXISTS lol_games.match (
              match_id varchar PRIMARY KEY,
              duration int
            )
            """
    outcome = """
              CREATE TABLE IF NOT EXISTS lol_games.outcome (
              outcome_id int PRIMARY KEY,
              outcome_name varchar
            )
            """
    performance = """
              CREATE TABLE IF NOT EXISTS lol_games.performance (
              performance_id serial PRIMARY KEY,
              match_id varchar not null references lol_games.match("match_id"),
              summoner_id varchar not null references lol_games.summoner("puuid"),
              champion_id int not null references lol_games.champion("champion_id"),
              kills int,
              deaths int,
              gold int,
              minions int,
              xp int,
              wards int,
              outcome_id int not null references lol_games.outcome("outcome_id"), 
              UNIQUE(match_id, summoner_id, champion_id)
            )
            """
    cursor.execute('CREATE SCHEMA IF NOT EXISTS lol_games')
    cursor.execute(country)
    cursor.execute(role)
    cursor.execute(champion)
    cursor.execute(streamer)
    cursor.execute(summoner)
    cursor.execute(match)
    cursor.execute(outcome)
    cursor.execute(performance)
    print('All Tables Created')

# commit changes
def commit_changes(conn):
    conn.commit()

# populate tables with psycopg2
def populate_db(cursor, country_tuples, role_tuples, champion_tuples, streamer_tuples, 
                summoner_tuples, match_tuples, outcome_tuples, performance_tuples):
    country_query = "INSERT INTO lol_games.country (country_name) VALUES (%s) ON CONFLICT DO NOTHING"
    role_query = "INSERT INTO lol_games.role (role_id, role_name) VALUES (%s, %s) ON CONFLICT DO NOTHING"
    champion_query = "INSERT INTO lol_games.champion (champion_id, champion_name, attack, defense, magic) \
                      VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"
    streamer_query = "INSERT INTO lol_games.streamer (streamer_id, streamer_name, real_name, birth_country, role_id) \
                      VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"
    summoner_query = "INSERT INTO lol_games.summoner (puuid, account_name, streamer_id) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING"
    match_query = "INSERT INTO lol_games.match (match_id, duration) VALUES (%s, %s) ON CONFLICT DO NOTHING"
    outcome_query = "INSERT INTO lol_games.outcome (outcome_id, outcome_name) VALUES (%s, %s) ON CONFLICT DO NOTHING"
    performance_query = "INSERT INTO lol_games.performance (match_id, summoner_id, champion_id, kills, deaths, gold, \
    minions, xp, wards, outcome_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"
    cursor.executemany(country_query, country_tuples)
    cursor.executemany(role_query, role_tuples)
    cursor.executemany(champion_query, champion_tuples)
    cursor.executemany(streamer_query, streamer_tuples)
    cursor.executemany(summoner_query, summoner_tuples)
    cursor.executemany(match_query, match_tuples)
    cursor.executemany(outcome_query, outcome_tuples)
    cursor.executemany(performance_query, performance_tuples)
    print("Done populating")

def get_table_from_postgres(sp, cursor, schema_name, table_name):
    cursor.execute("SELECT * FROM " + schema_name + "." + table_name)
    temp_df = cursor.fetchall()
    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s';" \
                   % (schema_name, table_name))
    rows = cursor.fetchall()
    column_names = [row[0] for row in rows]
    spark_df = sp.createDataFrame(temp_df, column_names)
    return spark_df