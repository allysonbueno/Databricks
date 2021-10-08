# Databricks notebook source
# Path - dataset1
path_dataset1 = "/FileStore/tables/carga/country_vaccinations.csv"

# Path - RDD
# path_rdd = "/FileStore/tables/arquivo_rdd.txt"

# COMMAND ----------

# Leitura de Dataframe

## Opção 1
# df1 = spark.read.format("csv").option("header","true").load(path_dataset1)
# df1 = spark.read.format("csv").option("header","true").option("InferSchema","true").load(path_dataset1)

## Opção 2
# CSV sempre vem separado por virgula entao nao precisa da opçao 1
df1 = spark.read.csv(path_dataset1)
df1 = spark.read.option("header","true").option("inferSchema","true").csv(path_dataset1)

# se quiser mudar o delimitador:
# df1 = spark.read.option("header","true").option("delimiter","|").option("inferSchema","true").csv(path_dataset1)


## Exibindo dataframe
# df1.show(2)
# df1.dtypes



# COMMAND ----------

# df1.write.format("parquet").save("/FileStore/tables/RAW_ZONE_PARQUET/")

# REPARTITION divide o arquivo pela quantidade que definir exemplo (2)
# com overwrite ele substitui os arquivos
df1.repartition(2).write.format("parquet").mode("overwrite").save("/FileStore/tables/RAW_ZONE_PARQUET/")

# com append ele adiciona sem substituir
#df1.repartition(2).write.format("parquet").mode("overwrite").save("/FileStore/tables/RAW_ZONE_PARQUET/")

# COMMAND ----------

df1.take(1)

# COMMAND ----------

# Ler RDD
path = "/FileStore/tables/arquivo_rdd.txt"

# criando um dataframe a partir de um JSON
dataframe = spark.read.json(path)

# Criado um dataframe a partir de um ORC
dataframe = spark.read.orc(path)

# criando um dataframe a partir de um PARQUET
dataframe = spark.read.parquet(path)

# COMMAND ----------

df1.dtypes

# COMMAND ----------

# Leitura de um RDD
rdd = sc.textfile(path_rdd)
# rdd.show X não é possivel utilizar show no RDD
# correto:
rdd.collect()

# COMMAND ----------

df1.take(1)

# COMMAND ----------

# Criando tabela temporaria
nomeTabelaTemporaria = "tempTableDataFrame1"
df1.createOrReplaceTempView(nomeTabelaTemporaria)

# Lendo a tabela temporaria
spark.read.table(nomeTabelaTemporaria).show()

# COMMAND ----------

# SELECT
# spark.sql("SELECT * FROM tempTableDataFrame1").show()
# display(spark.sql("SELECT country, iso_code FROM tempTableDataFrame1").show())
dfQuery = spark.sql("SELECT count(*) tt, country FROM tempTableDataFrame1 group by country")
dfQuery.show(5)

# COMMAND ----------

dfQuery.dtypes

# COMMAND ----------

# Scala
#import org.apache.spark.sql.functions._
# Python
from pyspark.sql.functions import col, column

# COMMAND ----------

# Usando function col ou column
# df1.select(col("country"), col("date"), col("iso_code")).show()

# Outra forma de fazer sql - Usando selectExpr
df1.selectExpr("country", "date", "iso_code").show()

# COMMAND ----------

# Scala import
# org.apache.spark.sql.types._
# Criando um Schema manualmente no PySpark
from pyspark.sql.types import *
# StructType: é um array de colunas, serve para utilizar um schema diferente  
# StructField: renomeia as colunas
dataframe_ficticio = StructType([
  StructField("col_String_1", StringType()),
  StructField("col_Integer_2", IntegerType()),
  StructField("col_Decimal_3", DecimalType())
])
dataframe_ficticio

# COMMAND ----------

# Função para gerar Schema (campos/colunas/nomes de colunas)
'''
# Scala
org.apache.spark.sql.types._
def getSchema(fields : Array[StructField]) : StructType = {
new StructType(fields)
}
'''
# PySpark
def getSchema(fields):
  return StructType(fields)


schema = getSchema([StructField("coluna1", StringType()), StructField("coluna2", StringType()), StructField("coluna3",StringType())])

#Show
df1.show(2)
#Take
df1.take(2)

# COMMAND ----------

# Gravando um novo CSV
path_destino="/FileStore/tables/CSV/"
nome_arquivo="arquivo.csv"
path_geral= path_destino + nome_arquivo
df1.write.format("csv").mode("overwrite").option("sep", "\t").save(path_geral)

# COMMAND ----------

# Gravando um novo JSON
path_destino="/FileStore/tables/JSON/"
nome_arquivo="arquivo.json"
path_geral= path_destino + nome_arquivo
df1.write.format("json").mode("overwrite").save(path_geral)

# COMMAND ----------

# Gravando um novo PARQUET
path_destino="/FileStore/tables/PARQUET/"
nome_arquivo="arquivo.parquet"
path_geral= path_destino + nome_arquivo
df1.write.format("parquet").mode("overwrite").save(path_geral)

# COMMAND ----------

# Gravando um novo ORC
path_destino="/FileStore/tables/ORC/"
nome_arquivo="arquivo.orc"
path_geral= path_destino + nome_arquivo
df1.write.format("orc").mode("overwrite").save(path_geral)

# COMMAND ----------

# Outros tipos de SELECT
# Diferentes formas de selecionar uma coluna
from pyspark.sql.functions import *
df1.select("country").show(5)
df1.select('country').show(5)
df1.select(col("country")).show(5)
#df1.select(column("country")).show(5)
df1.select(col("country")).show(5)
df1.select(expr("country")).show(5)

# COMMAND ----------

# Define uma nova coluna com um valor constante
    # funcao lit ta dentro do pacote functions e serve pra adicionar informação(coluna) constante ou alteranada 
    # UDF traz funções do python para spark
# TESTE INSERINDO COLUNA E ATRIBUINDO EM UM NOVO DATAFRAME
df2 = df1.withColumn("nova_coluna", lit(1))
display(df1)
display(df2)

# COMMAND ----------

# Adicionar coluna
teste = expr("total_vaccinations < 40")
df1.select("country", "total_vaccinations").withColumn("teste", teste).show(5)
# Renomear uma coluna
df1.select(expr("total_vaccinations as total_de_vacinados")).show(5)

# ALIAS - 
df1.select(col("country").alias("pais")).show(5)
df1.select("country").withColumnRenamed("country", "pais").show(5)
# Remover uma coluna
df3 = df1.drop("country")
df3.columns


# COMMAND ----------

# Filtrando dados e ordenando
# where() é um alias para filter().
# Seleciona apenas os primeiros registros da coluna "total_vaccinations"
df1.filter(df1.total_vaccinations > 55).orderBy(df1.total_vaccinations).show(2)
# Filtra por país igual Argentina
df1.select(df1.total_vaccinations, df1.country).filter(df1.country == "Argentina").show(5)
# Filtra por país diferente Argentina
df1.select(df1.total_vaccinations, df1.country).where(df1.country != "Argentina").show(5) # python type

# COMMAND ----------

# Filtrando dados e ordenando
# Mostra valores únicos
df1.select("country").distinct().show()
# Especificando vários filtros em comando separados
filtro_vacinas = df1.total_vaccinations < 100
filtro_pais = df1.country.contains("Argentina")
df1.select(df1.total_vaccinations, df1.country, df1.vaccines).where(df1.vaccines.isin("Sputnik V", "Sinovac")).filter(filtro_vacinas).show(5)
df1.select(df1.total_vaccinations, df1.country, df1.vaccines).where(df1.vaccines.isin("Sputnik V",
"Sinovac")).filter(filtro_vacinas).withColumn("filtro_pais", filtro_pais).show(5)

# COMMAND ----------

"""#######################################################################################################################
Convertendo dados
#######################################################################################################################"""
df5 = df1.withColumn("PAIS", col("country").cast("string").alias("PAIS"))
df5.select(df5.PAIS).show(2)
"""#######################################################################################################################
Trabalhando com funções
#######################################################################################################################"""
# Usando funções
df1.select(upper(df1.country)).show(3)
df1.select(lower(df1.country)).show(4)

# JOINS

# COMMAND ----------

# Criando um dataframe genérico
d = [{'name': 'Alice', 'age':1}]
df_A = spark.createDataFrame(d)
df_A.show()

# COMMAND ----------

rdd1 = [{"nome": "Marco", "idade": 33,"status": 'true'},
		{"nome": "Antonio", "idade": 33,"status": 'true'},
		{"nome": "Pereira", "idade": 33,"status": 'true'},
		{"nome": "Helena", "idade": 30,"status": 'true'},
		{"nome": "Fernando", "idade": 35,"status": 'true'},
		{"nome": "Carlos", "idade": 28,"status": 'true'},
		{"nome": "Lisa", "idade": 26,"status": 'true'},
		{"nome": "Candido", "idade": 75,"status": 'false'},
		{"nome": "Vasco", "idade": 62,"status": 'true'}]
dff1 = spark.createDataFrame(rdd1)
dff1.show()

rdd2 = [{"nome": "Marco", "PaisOrigem": "Brasil"},
		{"nome": "Helena", "PaisOrigem": "Brasil"},
		{"nome": "Gabriel", "PaisOrigem": "Brasil"},
		{"nome": "Vasco", "PaisOrigem": "Portugal"},
		{"nome": "Medhi", "PaisOrigem": "Marrocos"}]
dff2 = spark.createDataFrame(rdd2)
dff2.show() 

# join_type = "left_anti"    # tudo que não existe nos 2 grupos
# join_type = "left_semi"    # left join preferencia df da esquerda
# join_type = "right_outer"  # right join preferencia df da direita, mostrando também os nulos da esquerda
# join_type = "left_outer"   # left join preferencia df da esquerda, mostrando também os nulos da direita
# join_type = "full outer"   # mostra todas as combinações
# join_type = "cross join"   # cartesiano
join_type = "inner"          # inner join, mostra somente as combinações

join_condition = dff1.nome == dff2.nome
df3 = dff1.join(dff2, join_condition, join_type)
df3.show()
