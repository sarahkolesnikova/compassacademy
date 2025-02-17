# Script para criação da dimensão de streaming


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


# Criação de contexto spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Definição dos arquivos de origem e leitura dos dados
arquivo = args ['S3_INPUT_PATH']
dados = spark.read.parquet(arquivo)
arquivo1 = args ['S3_INPUT_PATH1']
filmes = spark.read.parquet(arquivo1)

# Filtragem dos ids do tmdb
ids = [row["id_tmdb"] for row in filmes.select("id_tmdb").collect()]

# Filtragem dos dados de streaming
filtered_strea = dados.filter(col("id").isin(ids))
print (filtered_strea.count())

# Separação e filtro dos dados de streaming dos dicionários que serão utlizados em colunas
streaming = filtered_strea.withColumn("content1", split(col("streaming"), "}").getItem(0))
streaming = streaming.withColumn("content2", split(col("streaming"), "}").getItem(1))
streaming = streaming.withColumn("content3", split(col("streaming"), "}").getItem(2))
streaming = streaming.withColumn("content4", split(col("streaming"), "}").getItem(3))
streaming = streaming.withColumn("content5", split(col("streaming"), "}").getItem(4))
streaming = streaming.withColumn("content6", split(col("streaming"), "}").getItem(5))
streaming = streaming.withColumn("content7", split(col("streaming"), "}").getItem(6))

# Organização dos dados filtrados em novas colunas
streaming = streaming.withColumn("provider_id_1", split(col("content1"), ",").getItem(1))
streaming = streaming.withColumn("provider_name", split(col("content1"), ",").getItem(2))
streaming = streaming.withColumn("display_priority_1", split(col("content1"), ",").getItem(3))
streaming = streaming.drop('content1')

# Tratamento dos dados para separação dos valores
streaming = streaming.withColumn("provider_id", split(col("provider_id_1"), ":").getItem(1))
streaming = streaming.drop('provider_id_1')
streaming = streaming.withColumn("provider_name_1", split(col("provider_name"), ":").getItem(1))
streaming = streaming.withColumn("display_priority", split(col("display_priority_1"), ":").getItem(1))
streaming = streaming.drop('provider_name')
streaming = streaming.drop('display_priority_1')

# Organização dos dados filtrados em novas colunas
streaming = streaming.withColumn("provider_id_2", split(col("content2"), ",").getItem(2))
streaming = streaming.withColumn("provider_name_2", split(col("content2"), ",").getItem(3))
streaming = streaming.withColumn("display_priority_1", split(col("content2"), ",").getItem(4))
streaming = streaming.drop('content2')

# Tratamento dos dados para separação dos valores
streaming = streaming.withColumn("provider_id_1", split(col("provider_id_2"), ":").getItem(1))
streaming = streaming.drop('provider_id_2')
streaming = streaming.withColumn("provider_name2", split(col("provider_name_2"), ":").getItem(1))
streaming = streaming.withColumn("display_priority1", split(col("display_priority_1"), ":").getItem(1))
streaming = streaming.drop('provider_name_2')
streaming = streaming.drop('display_priority_1')

# Organização dos dados filtrados em novas colunas
streaming = streaming.withColumn("provider_id_3", split(col("content3"), ",").getItem(2))
streaming = streaming.withColumn("provider_name_3", split(col("content3"), ",").getItem(3))
streaming = streaming.withColumn("display_priority_3", split(col("content3"), ",").getItem(4))
streaming = streaming.drop('content3')

# Tratamento dos dados para separação dos valores
streaming = streaming.withColumn("provider_id3", split(col("provider_id_3"), ":").getItem(1))
streaming = streaming.drop('provider_id_3')
streaming = streaming.withColumn("provider_name3", split(col("provider_name_3"), ":").getItem(1))
streaming = streaming.withColumn("display_priority3", split(col("display_priority_3"), ":").getItem(1))
streaming = streaming.drop('provider_name_3')
streaming = streaming.drop('display_priority_3')

# Organização dos dados filtrados em novas colunas
streaming = streaming.withColumn("provider_id_4", split(col("content4"), ",").getItem(2))
streaming = streaming.withColumn("provider_name_4", split(col("content4"), ",").getItem(3))
streaming = streaming.withColumn("display_priority_4", split(col("content4"), ",").getItem(4))
streaming = streaming.drop('content4')

# Tratamento dos dados para separação dos valores
streaming = streaming.withColumn("provider_id4", split(col("provider_id_4"), ":").getItem(1))
streaming = streaming.drop('provider_id_4')
streaming = streaming.withColumn("provider_name4", split(col("provider_name_4"), ":").getItem(1))
streaming = streaming.withColumn("display_priority4", split(col("display_priority_4"), ":").getItem(1))
streaming = streaming.drop('provider_name_4')
streaming = streaming.drop('display_priority_4')

# Organização dos dados filtrados em novas colunas
streaming = streaming.withColumn("provider_id_5", split(col("content5"), ",").getItem(2))
streaming = streaming.withColumn("provider_name_5", split(col("content5"), ",").getItem(3))
streaming = streaming.withColumn("display_priority_5", split(col("content5"), ",").getItem(4))
streaming = streaming.drop('content5')

# Tratamento dos dados para separação dos valores
streaming = streaming.withColumn("provider_id5", split(col("provider_id_5"), ":").getItem(1))
streaming = streaming.drop('provider_id_5')
streaming = streaming.withColumn("provider_name5", split(col("provider_name_5"), ":").getItem(1))
streaming = streaming.withColumn("display_priority5", split(col("display_priority_5"), ":").getItem(1))
streaming = streaming.drop('provider_name_5')
streaming = streaming.drop('display_priority_5')

# Organização dos dados filtrados em novas colunas
streaming = streaming.withColumn("provider_id_6", split(col("content6"), ",").getItem(2))
streaming = streaming.withColumn("provider_name_6", split(col("content6"), ",").getItem(3))
streaming = streaming.withColumn("display_priority_6", split(col("content6"), ",").getItem(4))
streaming = streaming.drop('content6')

# Tratamento dos dados para separação dos valores
streaming = streaming.withColumn("provider_id6", split(col("provider_id_6"), ":").getItem(1))
streaming = streaming.drop('provider_id_6')
streaming = streaming.withColumn("provider_name6", split(col("provider_name_6"), ":").getItem(1))
streaming = streaming.withColumn("display_priority6", split(col("display_priority_6"), ":").getItem(1))
streaming = streaming.drop('provider_name_6')
streaming = streaming.drop('display_priority_6')

# Organização dos dados filtrados em novas colunas
streaming = streaming.withColumn("provider_id_7", split(col("content7"), ",").getItem(2))
streaming = streaming.withColumn("provider_name_7", split(col("content7"), ",").getItem(3))
streaming = streaming.withColumn("display_priority_7", split(col("content7"), ",").getItem(4))
streaming = streaming.drop('content7')

# Tratamento dos dados para separação dos valores
streaming = streaming.withColumn("provider_id7", split(col("provider_id_7"), ":").getItem(1))
streaming = streaming.drop('provider_id_7')
streaming = streaming.withColumn("provider_name7", split(col("provider_name_7"), ":").getItem(1))
streaming = streaming.withColumn("display_priority7", split(col("display_priority_7"), ":").getItem(1))
streaming = streaming.drop('provider_name_7')
streaming = streaming.drop('display_priority_7')

# Criação de dataframes com os dados filtrados e tratados
provider1 = streaming.select("id","provider_id", "provider_name_1", "display_priority") # seleciona as colunas
provider1 = provider1.withColumn("provider_id", provider1["provider_id"].cast(IntegerType())) # converte o tipo da coluna
provider1.printSchema()
provider1 = provider1.na.drop(subset=["provider_id"]) # remove valores nulos
provider1 = provider1.orderBy("provider_id", ascending=True)  # ordena os valores
print (provider1.count())
exibicao = provider1.drop("provider_name_1") # remove a coluna
dim_streaming = provider1.drop("id", "display_priority") # remove as colunas
dim_streaming = dim_streaming.dropDuplicates() # remove valores duplicados

# O processo é repetido para os demais provedores de streaming
provider2 = streaming.select("id","provider_id_1", "provider_name2", "display_priority1")
provider2 = provider2.withColumn("provider_id_1", provider2["provider_id_1"].cast(IntegerType()))
provider2.printSchema()
provider2 = provider2.na.drop(subset=["provider_id_1"])
provider2 = provider2.orderBy("provider_id_1", ascending=True)
streaming2 = provider2.drop("id", "display_priority1")
streaming2 = streaming2.dropDuplicates()
provider3 = streaming.select("id","provider_id3", "provider_name3", "display_priority3")
provider3 = provider3.withColumn("provider_id3", provider3["provider_id3"].cast(IntegerType()))
provider3.printSchema()
provider3 = provider3.na.drop(subset=["provider_id3"])
provider3 = provider3.orderBy("provider_id3", ascending=True)
streaming3 = provider3.drop("id", "display_priority3")
streaming3 = streaming3.dropDuplicates()
provider4 = streaming.select("id","provider_id4", "provider_name4", "display_priority4")
provider4 = provider4.withColumn("provider_id4", provider4["provider_id4"].cast(IntegerType()))
provider4.printSchema()
provider4 = provider4.na.drop(subset=["provider_id4"])
provider4 = provider4.orderBy("provider_id4", ascending=True)
streaming4 = provider4.drop("id", "display_priority4")
streaming4 = streaming4.dropDuplicates()
provider5 = streaming.select("id","provider_id5", "provider_name5", "display_priority5")
provider5 = provider5.withColumn("provider_id5", provider5["provider_id5"].cast(IntegerType()))
provider5.printSchema()
provider5 = provider5.na.drop(subset=["provider_id5"])
provider5 = provider5.orderBy("provider_id5", ascending=True)
streaming5 = provider5.drop("id", "display_priority5")
streaming5 = streaming5.dropDuplicates()
provider6 = streaming.select("id","provider_id6", "provider_name6", "display_priority6")
provider6 = provider6.withColumn("provider_id6", provider6["provider_id6"].cast(IntegerType()))
provider6.printSchema()
provider6 = provider6.na.drop(subset=["provider_id6"])
provider6 = provider6.orderBy("provider_id6", ascending=True)
streaming6 = provider6.drop("id", "display_priority6")
streaming6 = streaming6.dropDuplicates()
provider7 = streaming.select("id","provider_id7", "provider_name7", "display_priority7")
provider7 = provider7.withColumn("provider_id7", provider7["provider_id7"].cast(IntegerType()))
provider7.printSchema()
provider7 = provider7.na.drop(subset=["provider_id7"])
provider7 = provider7.orderBy("provider_id7", ascending=True)
streaming7 = provider7.drop("id", "display_priority7")
streaming7 = streaming7.dropDuplicates()

# Renomeação das colunas
dim_streaming = dim_streaming.withColumnRenamed("provider_id", "id_streaming")
dim_streaming = dim_streaming.withColumnRenamed("provider_name_1", "nome_streaming")
streaming2 = streaming2.withColumnRenamed("provider_id_1", "id_streaming")
streaming2 = streaming2.withColumnRenamed("provider_name2", "nome_streaming")
streaming3 = streaming3.withColumnRenamed("provider_id3", "id_streaming")
streaming3 = streaming3.withColumnRenamed("provider_name3", "nome_streaming")
streaming4 = streaming4.withColumnRenamed("provider_id4", "id_streaming")
streaming4 = streaming4.withColumnRenamed("provider_name4", "nome_streaming")
streaming5 = streaming5.withColumnRenamed("provider_id5", "id_streaming")
streaming5 = streaming5.withColumnRenamed("provider_name5", "nome_streaming")
streaming6 =  streaming6.withColumnRenamed("provider_id6", "id_streaming")
streaming6 = streaming6.withColumnRenamed("provider_name6", "nome_streaming")
streaming7 = streaming7.withColumnRenamed("provider_id7", "id_streaming")
streaming7 = streaming7.withColumnRenamed("provider_name7", "nome_streaming")

# União dos dataframes
dim_streaming = dim_streaming.union(streaming2)
dim_streaming = dim_streaming.union(streaming3)
dim_streaming = dim_streaming.union(streaming4)
dim_streaming = dim_streaming.union(streaming5)
dim_streaming = dim_streaming.union(streaming6)
dim_streaming = dim_streaming.union(streaming7)

# Remoção de valores duplicados e ordenação dos valores
dim_streaming = dim_streaming.dropDuplicates()
print (dim_streaming.count())
dim_streaming = dim_streaming.orderBy("id_streaming", ascending=True)

# Escrita dos dados no S3
output = args['S3_TARGET_PATH']
df1 = dim_streaming
df_limited = DynamicFrame.fromDF(df1.limit(100000), glueContext, "df_limited")
glueContext.write_dynamic_frame.from_options(
        frame= df_limited,
        connection_type='s3',
        connection_options={"path": output},
        format="parquet"
        )

job.commit()