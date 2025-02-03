import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions

import pyspark.sql.functions as F


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


s3_path = "s3://exercicios-sarah/labglue/nomes.csv"
df = spark.read.option("header", "true").csv(s3_path)


df.printSchema()

df = df.withColumn("nome", F.upper(F.col("nome")))

print(f"Total rows: {df.count()}")


grouped_df = df.groupBy("ano", "sexo").count().orderBy(F.col("ano").desc())
grouped_df.show()


most_frequent_female = df.filter(df.sexo == 'F').groupBy("ano", "nome").count().orderBy(F.col("count").desc()).first()
print(f"Most frequent female name: {most_frequent_female['nome']} in year {most_frequent_female['ano']}")


most_frequent_male = df.filter(df.sexo == 'M').groupBy("ano", "nome").count().orderBy(F.col("count").desc()).first()
print(f"Most frequent male name: {most_frequent_male['nome']} in year {most_frequent_male['ano']}")


total_records_per_year = df.groupBy("ano").count().orderBy("ano").limit(10)
total_records_per_year.show()

output_path = "s3://lab-glue/frequencia_registro_nomes_eua"
df.write.partitionBy("sexo", "ano").mode("overwrite").json(output_path)

job.commit()