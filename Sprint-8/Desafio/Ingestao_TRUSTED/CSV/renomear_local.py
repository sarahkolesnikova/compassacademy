import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SOURCE_BUCKET', 'SOURCE_KEY1', 'SOURCE_KEY2', 'TARGET_KEY'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Variáveis
bucket_name = args['SOURCE_BUCKET']  # Nome do bucket
source_key1 = args['SOURCE_KEY1'] # Caminho do arquivo temporário da ingestão
source_key2 = args['SOURCE_KEY2'] # Caminho do arquivo temporário do tratamento
target_key = args['TARGET_KEY'] # Caminho final do arquivo tratado

# Inicializando o cliente do S3
s3 = boto3.client('s3')

# Excluindo o arquivo temporário da ingestão

s3.delete_object(
    Bucket=bucket_name,
    Key=source_key1
)

# Copiando o arquivo temporário do tratamento para o novo nome
s3.copy_object(
    Bucket=bucket_name,
    CopySource={'Bucket': bucket_name, 'Key': source_key2},
    Key=target_key
)

# Excluindo o arquivo temporário do tratamento
s3.delete_object(
    Bucket=bucket_name,
    Key=source_key2

job.commit()