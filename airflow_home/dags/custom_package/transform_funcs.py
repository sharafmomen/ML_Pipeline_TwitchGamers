
import os
os.environ["JAVA_HOME"] = "/project/jars_and_packages/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/project/jars_and_packages/spark-3.2.1-bin-hadoop3.2"
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct
from pyspark.sql import SQLContext
import pyarrow.parquet as pq

import numpy as np # linear algebra
import pandas as pd 
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
import sqlalchemy # copy pd dataframe to Redshift 

# Starting spark session
def start_spark():
    spark = SparkSession \
            .builder \
            .appName("PySpark App") \
            .getOrCreate()
    return spark
    
# Closing spark session
def close_spark(spark_sesh):
    spark_sesh.stop()

# Reading CSV files with Spark    
def spark_read_csv(sp, path_to_csv):
    '''
    Ensure to define the path to the specific csv beforehand 
    Returns df
    be sure to assign to a variable
    '''
    df = sp.read.csv(path_to_csv, header=True)
    return df    

# saving all of them as parquet (to be overwritten)
def save_pyspark_df(df, path, name_of_file):
    savefile = path + name_of_file + ".parquet"
    df.write.mode('overwrite').parquet(savefile)
