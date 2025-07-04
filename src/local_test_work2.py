

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
from dotenv import load_dotenv
import sys

import subprocess
from tempfile import mkdtemp



# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def download_with_s3cmd(s3_path, local_dir):
    """Скачивание файлов через s3cmd"""
    try:
        logger.info(f"Downloading {s3_path} to {local_dir}")
        os.makedirs(local_dir, exist_ok=True)
        # Скачиваем только один файл для теста
        subprocess.run([
            "s3cmd",
            "get",
            "--limit=1",
            s3_path + "part-*.parquet",
            local_dir + "/"
        ], check=True)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"s3cmd failed: {e}")
        return False

def main():
    # Конфигурация
    BUCKET = "fraud-detection-data-otus-2025"
    DATE = "2020-01-19"
    SAMPLE_SIZE = 5
    TEMP_DIR = mkdtemp(prefix="spark_s3_")
    OUTPUT_PATH = "./sample_data.parquet"
    
    try:
        # 1. Скачиваем только один файл через s3cmd
        s3_path = f"s3://{BUCKET}/parquet/{DATE}/"
        if not download_with_s3cmd(s3_path, TEMP_DIR):
            raise Exception("Failed to download data with s3cmd")

        # 2. Инициализируем Spark с настройками памяти
        logger.info("Initializing SparkSession with memory optimization")
        spark = SparkSession.builder \
            .appName("LocalS3Test") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.default.parallelism", "2") \
            .getOrCreate()

        # 3. Читаем скачанные данные (только один файл)
        parquet_file = [f for f in os.listdir(TEMP_DIR) if f.endswith('.parquet')][0]
        local_parquet = f"file://{os.path.join(TEMP_DIR, parquet_file)}"
        logger.info(f"Reading local parquet file: {local_parquet}")
        
        df = spark.read.parquet(local_parquet)
        
        # 4. Обработка данных с ограничением размера
        logger.info(f"Total rows in file: {df.count()}")
        sample_df = df.limit(SAMPLE_SIZE).cache()  # Кэшируем маленький датафрейм
        
        # 5. Сохранение результата с пониженной параллельностью
        logger.info(f"Saving sample to {OUTPUT_PATH}")
        sample_df.write \
            .option("maxRecordsPerFile", 1000) \
            .mode("overwrite") \
            .parquet(OUTPUT_PATH)
        
        # 6. Вывод информации
        logger.info("\nSample data:")
        sample_df.show(SAMPLE_SIZE, truncate=False)
        
    except Exception as e:
        logger.error(f"Error: {str(e)}", exc_info=True)
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped")
        # Очистка временных файлов
        subprocess.run(["rm", "-rf", TEMP_DIR])
        logger.info(f"Removed temp dir: {TEMP_DIR}")

if __name__ == "__main__":
    main()