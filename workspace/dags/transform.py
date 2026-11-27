'''
=================================================
Transform Script
Author  : Ryan Farid Pratama

This script applies data cleaning and schema transformation steps to a PySpark DataFrame according to the EDA conclusions.
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
from pyspark.sql.types import DoubleType, IntegerType, StringType

# Keep only the columns actually needed downstream so the loaded dataset
# fits inside the MongoDB Atlas free tier (512 MB storage limit).
KEEP_COLUMNS = ["year", "mill_trase_id", "product_type", "mill", "mill_group",
                "province_of_production", "province_of_production_trase_id", "kabupaten_of_production", 
                "product_type", "exporter_group", "importer_group", 
                "zero_deforestation_indonesia_palm_oil", "country_of_first_import", 
                "palm_oil_deforestation_annual_exposure", "volume", "total_emission_exposure"]

def transform_data(df):
    '''
    Cleaning data and transform scheme in PySpark DataFrame.

    Parameters:
    -------
    df : DataFrame
        Raw PySpark DataFrame resulting from the extraction stage
    Returns:
    -------
    DataFrame
        A cleaned PySpark DataFrame ready to be inserted into MongoDB.

    Example use:
    df_cleaned = transform_data(df_raw)
    '''
    # 1. Standardize column names to ALL CAPS (avoid all lowercase names)
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.upper())

    # 2. Delete rows if there are missing values ​​in the main ID column
    df_clean = df.dropna(subset=["mill_trase_id"])

    # 3. Filter invalid data (example: export volume must be > 0)
    df_clean = df_clean.filter(F.col("volume") > 0)

    # 4. Explicit casting of numeric & date data types
    df_clean = df_clean\
        .withColumn("year", F.col("year").cast(IntegerType()))\
        .withColumn("volume", F.col("volume").cast(DoubleType()))\
        .withColumn("zero_deforestation_indonesia_palm_oil", F.col("zero_deforestation_indonesia_palm_oil").cast(DoubleType()))\
        .withColumn("palm_oil_deforestation_annual_exposure", F.col("palm_oil_deforestation_annual_exposure").cast(DoubleType()))\
        .withColumn("total_emission_exposure", F.col("total_emission_exposure").cast(DoubleType()))

    # 5. Add a composite tracking ID child column if needed
    df_clean = df_clean.withColumn("transaction_route_id",F.concat_ws("-", F.col("year"), F.col("mill_trase_id")))

    # 6. Keep only the required columns to shrink storage footprint
    keep = [c for c in KEEP_COLUMNS if c in df_clean.columns] + ["transaction_route_id"]
    df_clean = df_clean.select(*keep)

    # 7. Drop duplicate rows (supply-chain routes often repeat)
    df_clean = df_clean.dropDuplicates()

    return df_clean

if __name__ == "__main__":
    from extract import extract_data

    # Transform function test independently
    raw_path = "raw_dataset.csv"
    df_raw = extract_data(raw_path)
    
    df_clean = transform_data(df_raw)

    print(f"Total rows before cleaning: {df_raw.count()}")
    print(f"Total rows after cleaning: {df_clean.count()}")
    
    print("\\n--- Preview Data Transform Result (Top 5 Rows) ---")
    df_clean.show(5)