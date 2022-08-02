# Предобработать файлы и положить их в hdfs 
# Вспомогательная ноутбки из лекции Вадима Заигрина
# https://github.com/vzaigrin/otus/blob/main/SparkML/Credit%20Card%20Customers/Python/notebooks/Spark%20ML%20Research.ipynb

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

# Создаем сессию
spark = SparkSession.builder.appName("Spark handle file and upload to hdfs")    .getOrCreate()

# Открываем файл 
raw = spark.read.option("header", "true").option("inferSchema", "true").csv("otus_test/BankChurners.csv")

# Убираем первую и последние две колонки
# YOUR CODE

# Перемножить Total_Trans_Ct на Total_Ct_Chng_Q4_Q1
# YOUR CODE

# Сложить столбец Avg_Utilization_Ratio с Customer_Age 
# YOUR CODE

# Посчитать признак заданный формулой
# YOUR CODE

# Отборать новые признаки и загрузить в HDFS
# YOUR CODE
