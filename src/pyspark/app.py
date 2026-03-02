from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# load the data
# df_device = spark.read.option('multiline', 'true').json('docs/files/device/devices_1.json') # lendo apenas 1 arquivo do diretório
df_device = spark.read.option('multiline', 'true').json('docs/files/device/*.json') # lendo todos os arquivos do diretório

# schema, columns, count and show the data
df_device.printSchema()

print(df_device.columns)

print(df_device.count())

df_device.show()

# select specific columns
df_device.select('id', 'model', 'platform').show()
df_device.selectExpr('id', 'model', 'platform as type').show()

# filter
df_device.filter(df_device.platform == 'Android 10').show()

# group by and count

df_device.groupBy('platform').count().show()    # quando existe uma ação (.show()) o Spark executa em memória mostra o resultado, caso contrário, ele apenas constrói o plano de execução