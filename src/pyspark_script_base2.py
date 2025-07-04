from argparse import ArgumentParser
from pyspark.sql import SparkSession
import sys

def process_parquet_files(source_path: str, output_path: str) -> None:
    spark = SparkSession.builder \
        .appName("parquet-row-counter") \
        .enableHiveSupport() \
        .getOrCreate()
    
    try:
        print(f"Reading data from: {source_path}")
        df = spark.read.parquet(source_path)
        
        if df.rdd.isEmpty():
            print("WARNING: No data found in source files!")
            empty_df = spark.createDataFrame([], "result LONG")
            empty_df.write.parquet(output_path)
            return
            
        row_count = df.count()
        print(f"Found {row_count} rows in dataset")
        
        result_df = spark.createDataFrame([(row_count,)], ["result"])
        
        print(f"Saving result to: {output_path}")
        result_df.write.mode("overwrite").parquet(output_path)
        print(f"Success! Row count saved: {row_count}")
        
    except Exception as e:
        print(f"ERROR: {str(e)}")
        sys.exit(1)

def main():
    parser = ArgumentParser()
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--date", required=True, help="Date folder in format YYYY-MM-DD")
    args = parser.parse_args()
    bucket_name = args.bucket
    date_folder = args.date

    input_path = f"s3a://{bucket_name}/data/input/{date_folder}/"
    output_path = f"s3a://{bucket_name}/output_data/{date_folder}_row_count.parquet"
    
    process_parquet_files(input_path, output_path)

if __name__ == "__main__":
    main()