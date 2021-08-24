import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, StructField, StructType
import pyspark.sql.functions as F

'''
This pyspark is used to scan through the party member list and check for uniqueness and encounter clear. 
It returns the ReportID and status of unique party composition 
'''


def party_transformer(input_loc, output_loc):
    schema = StructType([StructField("ReportID", StringType()),
                         StructField("EncounterStatus", IntegerType()),
                         StructField("CharacterID", StringType()),
                         StructField("ClassID", StringType()),
                         StructField("ClassRole", StringType()),
                         StructField("PartySpot", StringType()),
                         StructField("QueryDate", StringType()), ])

    df_raw = spark.read.option("header", True).schema(schema).csv(input_loc)
    df_clean = df_raw.dropna()

    # Group rows by ReportID and pivot on the ClassID to create a ClassID per ReportID
    df_draft = df_clean.groupBy("ReportID", "EncounterStatus", "ClassRole", "QueryDate").pivot("PartySpot").agg(F.first('ClassID'))

    # Check for uniqueness and for encounter clear. Drop PartySpot columns for unique 0 or 1.
    df_draft_check = df_draft.filter("EncounterStatus = 1")

    df_draft_checked = df_draft_check.withColumn("PartyStatus", F.when(F.col("1") == F.col("2") | F.col("3") == F.col("4") | F.col("5") == F.col("6") | F.col("7") == F.col("8"), "0").otherwise("1").cast(IntegerType()))

    # Selecting only the relevant columns from data set
    df_out = df_draft_checked.select(["ReportID", "PartyStatus",
         F.col("1").alias("spot_1"),
         F.col("2").alias("spot_2"),
         F.col("3").alias("spot_3"),
         F.col("4").alias("spot_4"),
         F.col("5").alias("spot_5"),
         F.col("6").alias("spot_6"),
         F.col("7").alias("spot_7"),
         F.col("8").alias("spot_8")])

    df_out.write.mode("overwrite").parquet(output_loc)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=str,
                        help='HDFS input', default='/draft')
    parser.add_argument('--output', type=str,
                        help='HDFS output', default='/output')
    args = parser.parse_args()
    spark = SparkSession.builder.appName('party transformer').getOrCreate()
    party_transformer(input_loc=args.input, output_loc=args.output)
