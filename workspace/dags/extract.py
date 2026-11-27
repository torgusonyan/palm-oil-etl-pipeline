'''
=================================================
Extract Script
Author  : Ryan Farid Pratama

This script is used to read (ingest) the raw dataset P2M3_ryan-farid_data_raw.csv using PySpark into a Spark DataFrame that is ready to be transformed.
=================================================
'''

'''Libraries import & environment setup'''
import os, sys
import warnings
warnings.filterwarnings("ignore")

from dotenv import load_dotenv
load_dotenv()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F                 
from pyspark.sql.window import Window                   
from pyspark.sql.functions import col, count, when

def extract_data(file_path: str):
    '''
    Read raw data of .csv file using PySpark

    Parameters:
    -------
    file_path : str
        The path to the location of the raw CSV file to be read.
    Returns:
    -------
    DataFrame
        Raw PySpark DataFrame from CSV file.

    Example use:
    df_raw = extract_data("raw_dataset.csv")
    '''
    # Initialize the SparkSession
    spark = SparkSession.builder.appName("Palm-Oil-Extract-Pipeline").getOrCreate()

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Raw data file not found at path: {file_path}")

    # Read raw CSV file into PySpark DataFrame
    df_spark = spark.read.csv(file_path, header=True, inferSchema=True)

    return df_spark

if __name__ == "__main__":
    # Extract function test independently
    raw_path = "raw_dataset.csv"
    df_test = extract_data(raw_path)
    print(f"Total Extracted Rows: {df_test.count()}")
    df_test.show(5)