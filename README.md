# OTUS. Практика. Конвейер подготовки данных

## План занятия

0. Разберем схему конвейера подготовки данных
1. Развернем инфраструктуру в Yandex.Cloud
2. Посмотрим на конфигурацию инфраструктуры в модулях Terraform
3. Разберем DAG-пайплайн с Airflow и исходный код
4. Посмотрим на все компоненты в рабочем режиме
5. Запустим пайплайн и посмотрим на результаты
6. *Бонусlkz 

7. Дополнительно в бакет
    1. для логгировния /src/log4j.propeties
    2. для данных /data/input/data.parquet
    3. для дагов   /dags/...
    4. для скриптов /src/...
8. копировать json в айрфлоу
9. копировать подготовленные данные в бакет
10. создать aws cli с теми же ключами что и s3cmd см notebooks/testaws.py
11. в вс загрузить  prendulum  и запускать кластер в вм


https://yandex.cloud/ru/docs/storage/tools/aws-cli

12. terraform state rm module.storage.yandex_storage_bucket.bucket
airflow-bucket1-cde46cdb06762b83

terraform state rm module.storage.yandex_storage_bucket.bucket

14. создать папку output в созданном бакете
s3cmd mb s3://airflow-bucket-a43ae73f57943938/data/input/

загрузим тестовые данные
15. ls -la ~/temp_parquet/
s3cmd put ~/temp_parquet/*.parquet s3://airflow-bucket-a43ae73f57943938/data/input/2020-01-19/
s3cmd put ~/temp_parquet1/*.parquet s3://airflow-bucket-a43ae73f57943938/data/input/2020-02-18/
s3cmd put ~/temp_parquet2/*.parquet s3://airflow-bucket-a43ae73f57943938/data/input/2020-03-19/


16. работа исполнена в 3 этапа
1. на локальном компе в виртсреде запущен спарк (см requiriments-spark) написан скрипт который на
    малом обьеме все делает : читает файл, делает операции, записыват результат, все логгируется
    src/local_test_work.py
2. делаем полную схему айрфлоу с дагами - кластер спарк, простые вычисления, дистрой кластера спарк для 1 итерации
    src/pyspark_script_base2.py
    
3. делаем  полную схему айрфлоу с дагами - кластер спарк,  вычисления, дистрой кластера спарк для 2 итерации с логами   
    src/pyspark_script_base2.py
    dags/spark_parquet_processing2.py

