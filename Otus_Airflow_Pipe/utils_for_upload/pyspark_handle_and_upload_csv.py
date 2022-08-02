from pyspark.sql import SparkSession
from pyspark.sql import functions as f

# Создаем сессию
spark = SparkSession.builder.appName("Spark handle file and upload to hdfs").getOrCreate()

# Открываем файл 
raw = spark.read.option("header", "true").option("inferSchema", "true").csv("otus_test/BankChurners.csv")

# Убираем первую и последние две колонки
columns = raw.columns
columnsLen = len(columns)
colsToDrop = columns[0].split() + columns[columnsLen-2:columnsLen]
df = raw.drop(*colsToDrop)

# Перемножить Total_Trans_Ct на Total_Ct_Chng_Q4_Q1
df.withColumn('features_1', col('Total_Ct_Chng_Q4_Q1')*col('Total_Trans_Ct'))

# Сложить столбец Avg_Utilization_Ratio с Customer_Age 
df.withColumn('features_2', col('Total_Ct_Chng_Q4_Q1') + col('Total_Trans_Ct'))

# Посчитать признак заданный формулой
df.withColumn('features_3', (col('Customer_Age') - col('Dependent_count'))/(col('Total_Relationship') + col('Months_on_book')))

# Отборать новые признаки и загрузить в HDFS
df.select('features_1', 'features_2','features_3').write.mode("overwrite").parquet('final_BankChurners.parquet')

