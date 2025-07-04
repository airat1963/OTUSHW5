#вот этот вариант скрипта работает на локальном компьютере import os
import subprocess
import os
from pyspark.sql import SparkSession


# Конфигурация
BUCKET = "fraud-detection-data-otus-2025"
DATE = "2020-01-19"
SAMPLE_SIZE = 5  # Количество строк для выборки
TEMP_DIR = "./temp_parquet"
OUTPUT_FILE = "./sample_data.parquet"

# 1. Создаем временную директорию
os.makedirs(TEMP_DIR, exist_ok=True)

# 2. Скачиваем один файл с бакета
print(f"Скачиваю один файл из s3://{BUCKET}/parquet/{DATE}/...")
try:
    subprocess.run([
        "s3cmd",
        "get",
        "--limit=1",  # Только один файл
        f"s3://{BUCKET}/parquet/{DATE}/part-*.parquet",
        TEMP_DIR + "/"
    ], check=True)
    
    # Находим скачанный файл
    parquet_file = [f for f in os.listdir(TEMP_DIR) if f.endswith('.parquet')][0]
    parquet_path = os.path.join(TEMP_DIR, parquet_file)
    print(f"Скачан файл: {parquet_path}")
    
except Exception as e:
    print(f"Ошибка при скачивании: {e}")
    exit(1)

# 3. Читаем только 5 строк через PySpark
print(f"\nЧитаю {SAMPLE_SIZE} строк из файла...")
spark = SparkSession.builder \
    .appName("DataSampleTest") \
    .getOrCreate()

try:
    # Читаем файл и берем только нужное количество строк
    df = spark.read.parquet(parquet_path).limit(SAMPLE_SIZE)
    
    # Сохраняем мини-выборку
    df.write.mode("overwrite").parquet(OUTPUT_FILE)
    print(f"\nУспешно! Результат сохранен в {OUTPUT_FILE}")
    
    # Выводим информацию о данных
    print("\nСхема данных:")
    df.printSchema()
    
    print("\nПример данных:")
    df.show(SAMPLE_SIZE, truncate=False)
    
    if "tx_fraud" in df.columns:
        print("\nРаспределение значений tx_fraud:")
        df.groupBy("tx_fraud").count().show()
    else:
        print("\nКолонка 'tx_fraud' не найдена. Доступные колонки:", df.columns)

except Exception as e:
    print(f"\nОшибка при обработке: {e}")
    raise

finally:
    spark.stop()
    # Очищаем временные файлы
    subprocess.run(["rm", "-rf", TEMP_DIR])
    print("\nВременные файлы удалены")  