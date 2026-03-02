from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW vw_device
USING org.apache.spark.sql.json
OPTIONS (path "/Users/victorleite/GitHub/series-spark-teste/docs/files/device/*.json", multiline "true")
""")

print(spark.catalog.listTables())    # listar as tabelas temporárias criadas

# select data

spark.sql("""SELECT * FROM vw_device LIMIT 10;""").show()