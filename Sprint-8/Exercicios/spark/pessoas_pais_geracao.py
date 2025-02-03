
##################

# Importando as bibliotecas

##################
from IPython import get_ipython
from IPython.display import display

#############

# Configurando o ambiente

#############

!apt-get update -qq

!apt-get install openjdk-8-jdk-headless -qq > /dev/null

!wget -q https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop2.7.tgz

!tar xf spark-3.1.2-bin-hadoop2.7.tgz

!pip install -q findspark

#############

#configurar as variáveis de ambiente

#############

import os

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"

os.environ["SPARK_HOME"] = "/content/spark-3.1.2-bin-hadoop2.7"

#############

# tornar o pyspark "importável"

#############

import findspark

findspark.init()

#############

# Iniciando uma sessão local

#############

from pyspark.sql import SparkSession
from pyspark import SparkContext, SQLContext

spark = SparkSession.builder.appName("Exercicio Intro").getOrCreate()

#############

# ETAPA 1: Ler arquivo txt

#############

df_nomes = spark.read.csv("/content/drive/MyDrive/Colab Notebooks/nomes.txt")

#############

# ETAPA 2: Renomear as colunas

#############

import pyspark.sql.functions as f

df_nomes = df_nomes.withColumnRenamed("_c0", "Nomes")

#############

# ETAPA 3: Gerar coluna escolaridade

#############

from pyspark.sql.types import IntegerType
df_nomes = df_nomes.withColumn("random_num", (f.rand()*3).cast(IntegerType()))
df_nomes = df_nomes.withColumn("Escolaridade",
                              f.when(f.col("random_num") == 0, "Fundamental")
                              .when(f.col("random_num") == 1, "Médio")
                              .otherwise("Superior"))
df_nomes = df_nomes.drop("random_num")

#############

# ETAPA 4: criar coluna País

#############

paises_america_sul = ['Argentina', 'Bolívia', 'Brasil', 'Chile', 'Colômbia', 
                      'Equador', 'Guiana', 'Paraguai', 'Peru', 'Suriname', 
                      'Uruguai', 'Venezuela', 'Guyana Francesa']

df_nomes = df_nomes.withColumn("indice_pais", (f.rand()*13).cast(IntegerType()))
df_nomes = df_nomes.withColumn("Pais",
                              f.when(f.col("indice_pais") == 0, "Argentina")
                              .when(f.col("indice_pais") == 1, "Bolívia")
                              .when(f.col("indice_pais") == 2, "Brasil")
                              .when(f.col("indice_pais") == 3, "Chile")
                              .when(f.col("indice_pais") == 4, "Colômbia")
                              .when(f.col("indice_pais") == 5, "Equador")
                              .when(f.col("indice_pais") == 6, "Guiana")
                              .when(f.col("indice_pais") == 7, "Paraguai")
                              .when(f.col("indice_pais") == 8, "Peru")
                              .when(f.col("indice_pais") == 9, "Suriname")
                              .when(f.col("indice_pais") == 10, "Uruguai")
                              .when(f.col("indice_pais") == 11, "Venezuela")
                              .otherwise("Guyana Francesa"))
df_nomes = df_nomes.drop("indice_pais")

#############

# ETAPA 5: criar coluna AnoNascimento

#############

df_nomes = df_nomes.withColumn("anos_aleatorios", (f.rand()*65).cast(IntegerType()))
df_nomes = df_nomes.withColumn("AnoNascimento", f.lit(1945)).withColumn("AnoNascimento", f.col("AnoNascimento") + f.col("anos_aleatorios"))
df_nomes = df_nomes.drop("anos_aleatorios")

#############

# ETAPA 6: criar dataframe df_select

#############

df_select = df_nomes.select("Nomes").filter(df_nomes.AnoNascimento >= 2001)

#############

# ETAPA 7: criar dataframe df_select usando SQL

#############

df_nomes.createOrReplaceTempView("pessoas")
spark.sql("SELECT Nomes FROM pessoas WHERE AnoNascimento >= 2001").show()

#############

# ETAPA 8: contar Millennials

#############

millennials_df = df_nomes.filter((f.col("AnoNascimento") >= 1980) & (f.col("AnoNascimento") <= 1994))
num_millennials = millennials_df.count()
print("Número de Millenials:", num_millennials)

#############

# ETAPA 9: contar Millennials usando SQL

#############

df_nomes.createOrReplaceTempView("df_nomes_view")

num_millennials = spark.sql("""
    SELECT COUNT(*) AS total_millennials
    FROM df_nomes_view
    WHERE AnoNascimento BETWEEN 1980 AND 1994
""").collect()[0][0]

print("Número de Millenials:", num_millennials)

#############

# ETAPA 10: criar dataframe df_pais_geracao

#############

query = """
SELECT
  pais,
  CASE
    WHEN AnoNascimento BETWEEN 1944 AND 1964 THEN 'Baby Boomers'
    WHEN AnoNascimento BETWEEN 1965 AND 1979 THEN 'Geração X'
    WHEN AnoNascimento BETWEEN 1980 AND 1994 THEN 'Millennials'
    WHEN AnoNascimento BETWEEN 1995 AND 2015 THEN 'Geração Z'
    ELSE 'Outras Gerações'
  END AS geracao,
  COUNT(*) AS quantidade
FROM pessoas
GROUP BY pais, geracao
ORDER BY pais, geracao, quantidade
"""
df_resultado = spark.sql(query)

df_resultado.show()