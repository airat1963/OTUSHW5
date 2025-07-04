import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
from dotenv import load_dotenv
import sys
from datetime import datetime
import subprocess
from tempfile import mkdtemp


# Конфигурация логирования
def setup_logging():
    log_dir = "./logs"
    os.makedirs(log_dir, exist_ok=True)
    
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"{log_dir}/data_sample_{current_time}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

def log_execution_time(func):
    """Декоратор для логирования времени выполнения функций"""
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        logger.info(f"Starting {func.__name__}...")
        result = func(*args, **kwargs)
        duration = datetime.now() - start_time
        logger.info(f"Finished {func.__name__} in {duration.total_seconds():.2f} seconds")
        return result
    return wrapper

@log_execution_time
def download_with_s3cmd(s3_path, local_dir):
    """Скачивание файлов через s3cmd"""
    try:
        logger.info(f"Downloading from {s3_path} to {local_dir}")
        os.makedirs(local_dir, exist_ok=True)
        
        # Логируем свободное место на диске
        disk_usage = os.statvfs(local_dir)
        free_space_gb = (disk_usage.f_bavail * disk_usage.f_frsize) / (1024**3)
        logger.info(f"Available disk space: {free_space_gb:.2f} GB")
        
        # Скачиваем только один файл
        result = subprocess.run([
            "s3cmd",
            "get",
            "--limit=1",
            s3_path + "part-*.parquet",
            local_dir + "/"
        ], check=True, capture_output=True, text=True)
        
        logger.debug(f"s3cmd output:\n{result.stdout}")
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"s3cmd failed with code {e.returncode}")
        logger.error(f"Error output:\n{e.stderr}")
        return False

@log_execution_time
def process_data(spark, input_path, output_path, sample_size):
    """Обработка данных и сохранение результата"""
    try:
        # Чтение данных
        logger.info(f"Reading parquet file from {input_path}")
        df = spark.read.parquet(input_path)
        logger.info(f"Schema:\n{df._jdf.schema().treeString()}")
        
        # Выборка данных
        logger.info(f"Taking sample of {sample_size} rows")
        sample_df = df.limit(sample_size).cache()
        logger.info(f"Sample count: {sample_df.count()}")
        
        # Сохранение
        logger.info(f"Saving sample to {output_path}")
        sample_df.write \
            .mode("overwrite") \
            .parquet(output_path)
        
        # Логирование примера данных
        logger.info("Sample data preview:")
        for line in sample_df._jdf.showString(sample_size, 20, False).split("\n"):
            logger.info(line)
            
        return True
        
    except Exception as e:
        logger.error("Data processing failed", exc_info=True)
        raise

def main():
    # Конфигурация
    config = {
        "BUCKET": "fraud-detection-data-otus-2025",
        "DATE": "2020-01-19",
        "SAMPLE_SIZE": 5,
        "OUTPUT_PATH": "./sample_data.parquet"
    }
    
    logger.info("Starting script with configuration:")
    for k, v in config.items():
        logger.info(f"{k}: {v}")
    
    TEMP_DIR = mkdtemp(prefix="spark_s3_")
    logger.info(f"Using temp dir: {TEMP_DIR}")
    
    spark = None
    try:
        # 1. Скачивание данных
        s3_path = f"s3://{config['BUCKET']}/parquet/{config['DATE']}/"
        if not download_with_s3cmd(s3_path, TEMP_DIR):
            raise Exception("Failed to download data")
        
        # 2. Инициализация Spark
        logger.info("Initializing SparkSession")
        spark = SparkSession.builder \
            .appName("DataSampler") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "1g") \
            .getOrCreate()
        
        # 3. Обработка данных
        parquet_file = [f for f in os.listdir(TEMP_DIR) if f.endswith('.parquet')][0]
        input_path = f"file://{os.path.join(TEMP_DIR, parquet_file)}"
        
        process_data(spark, input_path, config['OUTPUT_PATH'], config['SAMPLE_SIZE'])
        
        logger.info("Script completed successfully")
        
    except Exception as e:
        logger.critical("Script failed", exc_info=True)
        raise
        
    finally:
        # Завершающие операции
        if spark:
            spark.stop()
            logger.info("Spark session stopped")
        
        logger.info("Cleaning up temp files")
        subprocess.run(["rm", "-rf", TEMP_DIR], check=False)
        logger.info(f"Removed temp dir: {TEMP_DIR}")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.error("Unhandled exception in main", exc_info=True)
        sys.exit(1)
    finally:
        logging.shutdown()