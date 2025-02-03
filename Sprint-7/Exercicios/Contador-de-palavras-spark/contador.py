
from pyspark.sql import SparkSession 
from pyspark.sql.functions import explode, split, lower 


spark = SparkSession.builder.appName("ContarPalavras").getOrCreate()

dados = spark.read.text("/content/drive/MyDrive/Colab Notebooks/README.md")

palavras = dados.select(explode(split(lower(dados["value"]), r"\s+")).alias("palavras"))

palavras_filtradas = palavras.filter(palavras["palavras"] != "")

palavrasContadas = palavras_filtradas.groupBy("palavras").count()

palavrasContadasOrg = palavrasContadas.orderBy("count", ascending=False)

palavrasContadasOrg.show(palavrasContadasOrg.count())


spark.stop()