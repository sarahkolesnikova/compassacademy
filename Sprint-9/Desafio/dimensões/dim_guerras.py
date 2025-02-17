# Script para gerar a dimensão de guerras

# Importação de bibliotecas
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_TARGET_PATH'])

# iniciando o spark context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Definindo o caminho de saída
target_path = args['S3_TARGET_PATH']

# Criando o dataframe
dados= [{"id_guerra": 1, "nome_guerra": "Primeira Guerra"}, {"id_guerra": 2, "nome_guerra": "Segunda Guerra"}, {"id_guerra": 3, "nome_guerra": "Guerra do Iraque"}, {"id_guerra": 4, "nome_guerra": "Guerra do Afeganistão"}, {"id_guerra": 5, "nome_guerra": "Primeira ou Segunda Guerra"}]
df = spark.createDataFrame(dados)
df.show() 

# Escrevendo o dataframe no S3
df_limited = df.limit(100000)
df_limited = DynamicFrame.fromDF(df_limited, glueContext, "df_limited")
glueContext.write_dynamic_frame.from_options(
        frame= df_limited,
        connection_type='s3',
        connection_options={"path":target_path},
        format="parquet"
        )


job.commit()