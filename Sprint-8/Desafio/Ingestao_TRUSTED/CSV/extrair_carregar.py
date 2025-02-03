import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_TARGET_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Entrada e saída

source_file = args['S3_INPUT_PATH']
target_path = args['S3_TARGET_PATH']

# Criando um DynamicFrame
df = glueContext.create_dynamic_frame.from_options(
    "s3",
    {"paths": [source_file]},
    "csv",
    {"withHeader": True, "separator": "|"}
)

# Filtrando filmes com o gênero 'War'
guerra = df.filter(lambda row: 'War' in row['genero'])

# Convertendo DynamicFrame para DataFrame para usar o limit
guerra_df = guerra.toDF()

# Limitando os registros para 230000
guerra_limited_df = guerra_df.limit(230000)

# Convertendo de volta para DynamicFrame
guerra_limited = DynamicFrame.fromDF(guerra_limited_df, glueContext, "guerra_limited")

# Gravando o arquivo no caminho temporário do S3

glueContext.write_dynamic_frame.from_options(
        frame= guerra_limited,
        connection_type='s3',
        connection_options={"path":target_path},
        format="parquet"
        )


job.commit()