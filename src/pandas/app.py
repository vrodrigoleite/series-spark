# import and set config

from pyspark.sql import SparkSession

builder = SparkSession.builder.appName("app")

builder = builder.config("spark.sql.execution.arrow.pandas.enabled", "true")

builder.getOrCreate()

print(builder)

# pandas on spark   
import pyspark.pandas as ps

get_device = ps.read_json("/Users/victorleite/GitHub/series-spark-teste/docs/files/device/*.json", multiline=True)

print(get_device.head())

get_device.info()

get_device.spark.explain(mode="formatted")

# A mudança de pandas para spark.pandas reflete diretamente na forma como os dados são processados. 
# O pandas é uma biblioteca de manipulação de dados em memória (executa no mestre), 
# enquanto o spark.pandas é uma API que permite trabalhar com dados distribuídos usando o Apache Spark (executado em múltiplos nós).