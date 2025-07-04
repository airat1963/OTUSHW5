from pyspark.sql import SparkSession
import os

# Настройка доступа к S3
os.environ['AWS_ACCESS_KEY_ID'] = '<ваш_ключ_S3>'
os.environ['AWS_SECRET_ACCESS_KEY'] = '<ваш_секретный_ключ_S3>'
os.environ['AWS_ENDPOINT_URL'] = 'https://storage.yandexcloud.net'

# Инициализация Spark с поддержкой S3
spark = SparkSession.builder \
    .appName("LocalFraudTest") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", os.environ['AWS_ENDPOINT_URL']) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

try:
    # Чтение только 5 строк из паркет-файла
    df = spark.read.parquet("s3a://fraud-detection-data-otus-2025/parquet/2020-01-19/").limit(5)
    
    print("=== Sample data ===")
    df.show(5, truncate=False)
    
    # Проверка подсчета fraud_tx
    if "fraud_tx" in df.columns:
        counts = df.groupBy("fraud_tx").count()
        print("\n=== Fraud counts ===")
        counts.show()
    else:
        print("Column 'fraud_tx' not found! Available columns:", df.columns)
        
except Exception as e:
    print(f"\n!!! ERROR: {str(e)}")
    raise

finally:
    spark.stop()