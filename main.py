import numpy as np
import pandas as pd
import random
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import col, countDistinct, max, when, desc
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS

spark = SparkSession \
    .builder \
    .appName("SteamRecommendation") \
    .getOrCreate()

schema = StructType([
    StructField("appid", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("release_date", StringType(), True),
    StructField("developer", StringType(), True),
    StructField("publisher", StringType(), True),
    StructField("platforms", StringType(), True),
    StructField("categories", StringType(), True),
    StructField("genres", StringType(), True),
    StructField("positive_ratings", IntegerType(), True),
    StructField("negative_ratings", IntegerType(), True),
    StructField("average_playtime", IntegerType(), True),
    StructField("median_playtime", IntegerType(), True),
    StructField("owners", StringType(), True),
    StructField("min_owners", IntegerType(), True),
    StructField("max_owners", IntegerType(), True),
    StructField("price", FloatType(), True)
])

df = spark.read.csv("hdfs://localhost:9000/esgi/steam/steam.csv",
                    header='true',
                    schema=schema,
                    sep=";")

df.printSchema()

df.show()