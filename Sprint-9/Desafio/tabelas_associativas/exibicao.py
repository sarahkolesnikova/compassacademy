# Script para criar a tabela associativa entre os filmes e os serviços de streaming

# Importação das bibliotecas
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, round, when, regexp_replace, split
from pyspark.sql.types import IntegerType
import pandas as pd
from functools import reduce
from pyspark.sql import DataFrame
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_TARGET_PATH', 'S3_INPUT_PATH', 'S3_INPUT_PATH1'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Carregando e lendo os dados dos filmes e dos serviços de streaming
arquivo = args ['S3_INPUT_PATH']
dados = spark.read.parquet(arquivo)
arquivo1 = args ['S3_INPUT_PATH1']
filmes = spark.read.parquet(arquivo1)

# Selecionando os ids dos filmes
ids = [row["id_tmdb"] for row in filmes.select("id_tmdb").collect()]

# Filtrando os dados dos stramings
filtered_strea = dados.filter(col("id").isin(ids))
print (filtered_strea.count())

# Criando a dimensão de streaming
streaming = filtered_strea.withColumn("content1", split(col("streaming"), "}").getItem(0))
streaming = streaming.withColumn("content2", split(col("streaming"), "}").getItem(1))
streaming = streaming.withColumn("content3", split(col("streaming"), "}").getItem(2))
streaming = streaming.withColumn("content4", split(col("streaming"), "}").getItem(3))
streaming = streaming.withColumn("content5", split(col("streaming"), "}").getItem(4))
streaming = streaming.withColumn("content6", split(col("streaming"), "}").getItem(5))
streaming = streaming.withColumn("content7", split(col("streaming"), "}").getItem(6))
streaming = streaming.withColumn("provider_id_1", split(col("content1"), ",").getItem(1))
streaming = streaming.withColumn("provider_name", split(col("content1"), ",").getItem(2))
streaming = streaming.withColumn("display_priority_1", split(col("content1"), ",").getItem(3))
streaming = streaming.drop('content1')
streaming = streaming.withColumn("provider_id", split(col("provider_id_1"), ":").getItem(1))
streaming = streaming.drop('provider_id_1')
streaming = streaming.withColumn("provider_name_1", split(col("provider_name"), ":").getItem(1))
streaming = streaming.withColumn("display_priority", split(col("display_priority_1"), ":").getItem(1))
streaming = streaming.drop('provider_name')
streaming = streaming.drop('display_priority_1')
streaming = streaming.withColumn("provider_id_2", split(col("content2"), ",").getItem(2))
streaming = streaming.withColumn("provider_name_2", split(col("content2"), ",").getItem(3))
streaming = streaming.withColumn("display_priority_1", split(col("content2"), ",").getItem(4))
streaming = streaming.drop('content2')
streaming = streaming.withColumn("provider_id_1", split(col("provider_id_2"), ":").getItem(1))
streaming = streaming.drop('provider_id_2')
streaming = streaming.withColumn("provider_name2", split(col("provider_name_2"), ":").getItem(1))
streaming = streaming.withColumn("display_priority1", split(col("display_priority_1"), ":").getItem(1))
streaming = streaming.drop('provider_name_2')
streaming = streaming.drop('display_priority_1')
streaming = streaming.withColumn("provider_id_3", split(col("content3"), ",").getItem(2))
streaming = streaming.withColumn("provider_name_3", split(col("content3"), ",").getItem(3))
streaming = streaming.withColumn("display_priority_3", split(col("content3"), ",").getItem(4))
streaming = streaming.drop('content3')
streaming = streaming.withColumn("provider_id3", split(col("provider_id_3"), ":").getItem(1))
streaming = streaming.drop('provider_id_3')
streaming = streaming.withColumn("provider_name3", split(col("provider_name_3"), ":").getItem(1))
streaming = streaming.withColumn("display_priority3", split(col("display_priority_3"), ":").getItem(1))
streaming = streaming.drop('provider_name_3')
streaming = streaming.drop('display_priority_3')
streaming = streaming.withColumn("provider_id_4", split(col("content4"), ",").getItem(2))
streaming = streaming.withColumn("provider_name_4", split(col("content4"), ",").getItem(3))
streaming = streaming.withColumn("display_priority_4", split(col("content4"), ",").getItem(4))
streaming = streaming.drop('content4')
streaming = streaming.withColumn("provider_id4", split(col("provider_id_4"), ":").getItem(1))
streaming = streaming.drop('provider_id_4')
streaming = streaming.withColumn("provider_name4", split(col("provider_name_4"), ":").getItem(1))
streaming = streaming.withColumn("display_priority4", split(col("display_priority_4"), ":").getItem(1))
streaming = streaming.drop('provider_name_4')
streaming = streaming.drop('display_priority_4')
streaming = streaming.withColumn("provider_id_5", split(col("content5"), ",").getItem(2))
streaming = streaming.withColumn("provider_name_5", split(col("content5"), ",").getItem(3))
streaming = streaming.withColumn("display_priority_5", split(col("content5"), ",").getItem(4))
streaming = streaming.drop('content5')
streaming = streaming.withColumn("provider_id5", split(col("provider_id_5"), ":").getItem(1))
streaming = streaming.drop('provider_id_5')
streaming = streaming.withColumn("provider_name5", split(col("provider_name_5"), ":").getItem(1))
streaming = streaming.withColumn("display_priority5", split(col("display_priority_5"), ":").getItem(1))
streaming = streaming.drop('provider_name_5')
streaming = streaming.drop('display_priority_5')
streaming = streaming.withColumn("provider_id_6", split(col("content6"), ",").getItem(2))
streaming = streaming.withColumn("provider_name_6", split(col("content6"), ",").getItem(3))
streaming = streaming.withColumn("display_priority_6", split(col("content6"), ",").getItem(4))
streaming = streaming.drop('content6')
streaming = streaming.withColumn("provider_id6", split(col("provider_id_6"), ":").getItem(1))
streaming = streaming.drop('provider_id_6')
streaming = streaming.withColumn("provider_name6", split(col("provider_name_6"), ":").getItem(1))
streaming = streaming.withColumn("display_priority6", split(col("display_priority_6"), ":").getItem(1))
streaming = streaming.drop('provider_name_6')
streaming = streaming.drop('display_priority_6')
streaming = streaming.withColumn("provider_id_7", split(col("content7"), ",").getItem(2))
streaming = streaming.withColumn("provider_name_7", split(col("content7"), ",").getItem(3))
streaming = streaming.withColumn("display_priority_7", split(col("content7"), ",").getItem(4))
streaming = streaming.drop('content7')
streaming = streaming.withColumn("provider_id7", split(col("provider_id_7"), ":").getItem(1))
streaming = streaming.drop('provider_id_7')
streaming = streaming.withColumn("provider_name7", split(col("provider_name_7"), ":").getItem(1))
streaming = streaming.withColumn("display_priority7", split(col("display_priority_7"), ":").getItem(1))
streaming = streaming.drop('provider_name_7')
streaming = streaming.drop('display_priority_7')

# Criando as tabelas com os dados dos serviços de streaming
provider1 = streaming.select("id","provider_id", "provider_name_1", "display_priority")
provider1 = provider1.withColumn("provider_id", provider1["provider_id"].cast(IntegerType()))
provider1.printSchema()
provider1 = provider1.orderBy("provider_id", ascending=True)
provider1 = provider1.na.drop(subset=["provider_id"])
print (provider1.count())
exibicao1 = provider1.drop("provider_name_1")

provider2 = streaming.select("id","provider_id_1", "provider_name2", "display_priority1")
provider2 = provider2.withColumn("provider_id_1", provider2["provider_id_1"].cast(IntegerType()))
provider2.printSchema()
provider2 = provider2.orderBy("provider_id_1", ascending=True)
provider2 = provider2.na.drop(subset=["provider_id_1"])
exibicao2 = provider2.drop("provider_name2")

provider3 = streaming.select("id","provider_id3", "provider_name3", "display_priority3")
provider3 = provider3.withColumn("provider_id3", provider3["provider_id3"].cast(IntegerType()))
provider3.printSchema()
provider3 = provider3.orderBy("provider_id3", ascending=True)
provider3 = provider3.na.drop(subset=["provider_id3"])
exibicao3 = provider3.drop("provider_name3")


provider4 = streaming.select("id","provider_id4", "provider_name4", "display_priority4")
provider4 = provider4.withColumn("provider_id4", provider4["provider_id4"].cast(IntegerType()))
provider4.printSchema()
provider4 = provider4.orderBy("provider_id4", ascending=True)
provider4 = provider4.na.drop(subset=["provider_id4"])
exibicao4 = provider4.drop("provider_name4")

provider5 = streaming.select("id","provider_id5", "provider_name5", "display_priority5")
provider5 = provider5.withColumn("provider_id5", provider5["provider_id5"].cast(IntegerType()))
provider5.printSchema()
provider5 = provider5.orderBy("provider_id5", ascending=True)
provider5 = provider5.na.drop(subset=["provider_id5"])
exibicao5 = provider5.drop("provider_name5")

provider6 = streaming.select("id","provider_id6", "provider_name6", "display_priority6")
provider6 = provider6.withColumn("provider_id6", provider6["provider_id6"].cast(IntegerType()))
provider6.printSchema()
provider6 = provider6.orderBy("provider_id6", ascending=True)
provider6 = provider6.na.drop(subset=["provider_id6"])
exibicao6 = provider6.drop("provider_name6")

provider7 = streaming.select("id","provider_id7", "provider_name7", "display_priority7")
provider7 = provider7.withColumn("provider_id7", provider7["provider_id7"].cast(IntegerType()))
provider7.printSchema()
provider7 = provider7.orderBy("provider_id7", ascending=True)
provider7 = provider7.na.drop(subset=["provider_id7"])
exibicao7 = provider7.drop("provider_name7")

# Renomeando as colunas
exibicao1 = exibicao1.withColumnRenamed("provider_id", "id_streaming")
exibicao1 = exibicao1.withColumnRenamed("id", "id_tmdb")
exibicao1 = exibicao1.withColumnRenamed("display_priority", "display_priority")


exibicao2 = exibicao2.withColumnRenamed("provider_id_1", "id_streaming")
exibicao2 = exibicao2.withColumnRenamed("display_priority1", "display_priority")
exibicao2 = exibicao2.withColumnRenamed("id", "id_tmdb")

exibicao3 = exibicao3.withColumnRenamed("provider_id3", "id_streaming")
exibicao3 = exibicao3.withColumnRenamed("id", "id_tmdb")
exibicao3 = exibicao3.withColumnRenamed("display_priority3", "display_priority")

exibicao4 = exibicao4.withColumnRenamed("provider_id4", "id_streaming")
exibicao4 = exibicao4.withColumnRenamed("id", "id_tmdb")
exibicao4 = exibicao4.withColumnRenamed("display_priority4", "display_priority")

exibicao5 = exibicao5.withColumnRenamed("provider_id5", "id_streaming")
exibicao5 = exibicao5.withColumnRenamed("id", "id_tmdb")
exibicao5 = exibicao5.withColumnRenamed("display_priority5", "display_priority")

exibicao6 = exibicao6.withColumnRenamed("provider_id6", "id_streaming")
exibicao6 = exibicao6.withColumnRenamed("id", "id_tmdb")
exibicao6 = exibicao6.withColumnRenamed("display_priority6", "display_priority")

exibicao7 = exibicao7.withColumnRenamed("provider_id7", "id_streaming")
exibicao7 = exibicao7.withColumnRenamed("id", "id_tmdb")
exibicao7 = exibicao7.withColumnRenamed("display_priority7", "display_priority")

# Unindo as tabelas para criar a tabela associativa
exibicao = exibicao1.union(exibicao2)
exibicao = exibicao.union(exibicao3)
exibicao = exibicao.union(exibicao4)
exibicao = exibicao.union(exibicao5)
exibicao = exibicao.union(exibicao6)
exibicao = exibicao.union(exibicao7)
print (exibicao.count())

# Ordenando a tabela
exibicao = exibicao.orderBy("id_streaming", ascending=True)

# Escrevendo os dados no S3
output = args['S3_TARGET_PATH']
df1 = exibicao
df_limited = DynamicFrame.fromDF(df1.limit(100000), glueContext, "df_limited")
glueContext.write_dynamic_frame.from_options(
        frame= df_limited,
        connection_type='s3',
        connection_options={"path": output},
        format="parquet"
        )

job.commit()