# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## 1 - Leitura das Bases

# COMMAND ----------

# Visualizando a Tabela Orders da Base de Dados Bronze

df_orders = spark.sql("SELECT * FROM bronze.orders")

df_orders.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2 - Conexão Bases Externas 

# COMMAND ----------

base = "server-estoque"
database_host = "server-estoque.database.windows.net"
database_port = "1433"
database_name = "estoque"
table = "dbo.posicao_estoque"
user = "root-estoque"
password = "Amb@!Stock"

# COMMAND ----------

url = f"jdbc:sqlserver://{database_host}:{database_port};database={database_name};user={user}@{base};password={password};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

url

# COMMAND ----------

driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

# COMMAND ----------

# SQL SERVER

posicao_estoque = (spark.read\
                   .format('jdbc')\
                   .option('driver', driver)\
                   .option('url', url)\
                   .option('dbtable', table)\
                   .option('user', user)\
                   .option('password', password)\
                   .load())

posicao_estoque.display()

# COMMAND ----------

# ORACLE

# https://www.cdata.com/kb/tech/oracledb-jdbc-azure-databricks.rst (OCI)

df_oracle = (spark.read\
    .format("jdbc")\
    .option("url", "jdbc:oracle:thin:username/password@//hostname:portnumber/SID")\
    .option("dbtable", "hr.emp")\
    .option("user", "db_user_name")\
    .option("password", "password")\
    .option("driver", "oracle.jdbc.driver.OracleDriver")\
    .load())

df_oracle.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 3 - Visualizando colunas (SELECT)

# COMMAND ----------

df_orders = spark.sql("SELECT * FROM bronze.orders")
df_orders.display()

# COMMAND ----------

spark.sql("SELECT * FROM bronze.orders").display()

# COMMAND ----------

df_orders.select(['order_id', 'store_id']).display()

# COMMAND ----------

# df_orders.select(['order_id', 'store_id']) (Neste modo mostra somente o tipo da coluna)

df_orders.select(['order_id', 'store_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 4 - Filtrando Valores (WHERE - Filter())

# COMMAND ----------

# Importando funções sql para o Pyspark
# Padrão functions as F or f

from pyspark.sql import functions as F


# COMMAND ----------

# No Pyspark temos a função col() que ajuda a selecionar colunas
# Com a função functions.col não precisa passar o dataframe toda hora para fazer seleção 

df_orders.filter(F.col('order_amount') > 100).display()

# COMMAND ----------

df_orders.filter((F.col('order_amount') > 100) &
                 (F.col('channel_id') == 1)).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 5 - Fazendo junções (JOIN)

# COMMAND ----------

# Verificando a tabela bronze.stores

df_full_stores = spark.sql("SELECT * FROM bronze.stores")

df_full_stores.display()


# COMMAND ----------

df_stores = spark.sql("SELECT store_id, store_name FROM bronze.stores")

df_stores.display()

# COMMAND ----------

# Fazendo join de todas as colunas
# how = pra indicar o tipo de Join
# on = pra indicar qual a coluna que faz o relacionamento entre as tabelas

df_orders.join(df_stores, on=['store_id'], how='left').display()


# COMMAND ----------

# df_orders_join = df_orders[['store_id', 'order_id', 'order_amount']].join(df_stores[['store_id']], on=['store_id'], how='left') (Chamando somente a coluna store_id do dataframe df_stores)

df_orders_join = df_orders[['store_id', 'order_id', 'order_amount']].join(df_stores, on=['store_id'], how='left')

df_orders_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 6 - Calculos Básicos e Agregações

# COMMAND ----------

df_orders = spark.sql("SELECT * FROM bronze.orders")
df_orders.display()

# COMMAND ----------

# withColumn(): https://sparkbyexamples.com/pyspark/pyspark-withcolumn/

'''
OBS: Tratamento de colunas usa o withColumn()

PySpark withColumn()é uma função de transformação do DataFrame que é usada para alterar o valor, 
converter o tipo de dados de uma coluna existente, criar uma nova coluna e muito mais

'''

# order_delivery_fee = comissão,gratificação pela entrega
# order_delivery_cost = custo direto pela entrega
# total_delivery_cost = nova coluna formada da soma de (order_delivery_fee + order_delivery_cost)

df_orders.withColumn('total_delivery_cost', F.col('order_delivery_fee') + F.col('order_delivery_cost')).display()


# COMMAND ----------

df_orders[['order_id', 'store_id', 'order_delivery_fee', 'order_delivery_cost']]\
    .withColumn('total_delivery_cost', F.col('order_delivery_fee') + F.col('order_delivery_cost'))\
    .display()

# COMMAND ----------

# Agregações exemplo 1 (withColumnRenamed) --> https://sparkbyexamples.com/pyspark/pyspark-groupby-agg-aggregate-explained/

# df_orders.groupBy(['store_id']) or df_orders.groupBy('store_id')

# {'chave':'valor'} = {'coluna':'função_agregação'}

df_orders.groupBy(['store_id'])\
    .agg({'order_amount':'sum'})\
    .withColumnRenamed('sum(order_amount)', 'total').display()  # Se não renomear o nome da coluna fica sum(order_amount)

# COMMAND ----------

# Agregações exemplo 2 (F.function_aggregation().alias())  --> from pyspark.sql import functions as F

df_orders.groupBy('store_id')\
    .agg(F.sum('order_amount').alias('total')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 7 - Tratamento de Dados Duplicados

# COMMAND ----------

df_orders = spark.sql("SELECT * FROM bronze.orders")
df_orders.display()

# COMMAND ----------

# Tem de passar o dataframe novamente para que ele agora receba a deleção dos dados duplicados

# Coalesce faz a junção das partições em somente 1 para otmização de deletar os dados duplicados

# https://www.geeksforgeeks.org/python-pandas-dataframe-drop_duplicates/

# OBS PANDAS: Temos a opção de selecionar as colunas que queremos deletar dados duplicados com 'subset' --> df.drop_duplicates(subset="name_column")

# Pyspark: dropDuplicates(['order_id','store_id'])

df_orders = df_orders.coalesce(1).dropDuplicates()

# COMMAND ----------

'''

EXEMPLO SQL + PYSPARK:


1: Criando um novo dataframe atavés de um Select no Spark/Pyspark

orders2 = spark.sql("SELECT * FROM bronze.orders LIMIT 100")
display(orders2)


2: Duplicando os dados com o novo datraframe orders2 criado, aumentando em 100 linhas duplicadas no dataframe original 

orders2.write.format("delta").mode("append").saveAsTable("bronze.orders")



3: Verificar quais são as order_id duplicadas


SELECT order_id, store_id, order_amount, COUNT(*) as qtd_registros_duplicados FROM bronze.orders GROUP BY order_id, store_id, order_amount;



4: Verificar a quantidade de linhas duplicadas de order_id (SUBQUERIES - SUBSELECT)

SELECT COUNT(*) AS Total_Duplicados
FROM(SELECT order_id, store_id, order_amount, COUNT(*) as qtd_registros_duplicados FROM bronze.orders GROUP BY order_id, store_id, order_amount) a
WHERE a.qtd_registros_duplicados > 1;




5: Criando um dataframe através do 'SELECT DISTINCT(*)' para trazer informações sem  os dados duplicados


df_orders = spark.sql(SELECT DISTINCT(*) FROM bronze.orders) 




'''

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 8 - Casting

# COMMAND ----------

# Importando as bibliotcas

# pyspark.sql.types (Importar todos os tipos de dados do pyspark-sql)

# Funções Replace Pyspark: https://sparkbyexamples.com/pyspark/pyspark-replace-column-values/

from pyspark.sql import functions as F
from pyspark.sql.types import *  

# COMMAND ----------

df_orders = spark.sql("SELECT * FROM bronze.orders")
df_orders.display()

# COMMAND ----------

df_orders[['order_amount', 'order_moment_created']] \
    .withColumn('order_amount', F.col('order_amount').cast(DoubleType())) \
    .withColumn('order_moment_created', F.to_date(F.regexp_replace(F.substring(F.col('order_moment_created'), 1, 9), ' ', ''), 'M/d/yyyy')) \
.display()

# COMMAND ----------

'''

USANDO SQL:

Exemplo 0:

SELECT
  CAST(order_amount AS FLOAT) AS order_amount,
  TO_DATE(REPLACE(SUBSTRING(order_moment_created, 1, 9), " ", ""), "M/d/yyyy") AS order_moment_created
FROM bronze.orders;


Exemplo 1:

SELECT
  CAST(order_amount AS FLOAT) AS order_amount,
  order_moment_created,
  TO_DATE(REPLACE(SUBSTRING(order_moment_created, 1, 9), " ", ""), "M/d/yyyy") AS order_moment_created_formated
FROM bronze.orders;


Exemplo 2:

SELECT
  CAST(order_amount AS FLOAT) AS order_amount,
  order_moment_created,
  TO_DATE(REPLACE(SUBSTRING(order_moment_created, 1, 9), " ", ""), "M/d/yyyy") AS order_moment_created_formated
FROM bronze.orders
WHERE
  order_created_day > 10
AND
  order_created_month > 2;



'''

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 9 - Detectando Anomalias

# COMMAND ----------

# 1 - Importar bibliotecas

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# 2 - Ver o dataframe completo

df_orders = spark.sql("SELECT * FROM bronze.orders")
df_orders.display()

# COMMAND ----------

# 3 - Fazer o casting na coluna desejada

df_orders = df_orders.withColumn('order_amount', F.col('order_amount').cast(DoubleType()))

# COMMAND ----------

# 4 - Verificando o tipos de dados das colunas com o 'printSchema()'

df_orders.printSchema()

# COMMAND ----------

# 5 - Verifcando os dados da coluna --> df[['name_column']]

display(df_orders[['order_amount']])

# COMMAND ----------

# 6 - Usando a função describe para verificar e detectar as anomalias 

# OBS: O describe() é parecido ao usar no SQL o ntitle


df_orders.describe().display()

# COMMAND ----------

'''

SQL ntitle() vs Pandas describe()

SELECT
  a.ntile,
  min(order_amount) as limite_inferior,
  max(order_amount) as limite_superior,
  avg(order_amount) as media,
  count(order_id) as orders
FROM
  (
    SELECT order_id, CAST(order_amount as FLOAT),
    ntile(4) OVER (ORDER BY CAST(order_amount as FLOAT)) as ntile
    FROM bronze.orders
  ) a
GROUP BY 1;


'''

# COMMAND ----------

# 7 - Verificando order_amount < 10000

# Ajuda a verificar esta anomalia para fazer o tratamento posterior

df_orders.filter(F.col('order_amount') < 10000).display()


# COMMAND ----------

# Importar bibliotecas

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# Função distinct()

# Verificar quais são os channel_id (Valores únicos) - 39 linhas

df_orders[['channel_id']].distinct().display()

# COMMAND ----------

# Função isin()

# 1, 10 e 11 --> São os 3 primeiros channel_id

# SQL = IN

# PANDAS = isin

df_orders_filter = df_orders[['order_id', 'store_id', 'order_amount', 'channel_id']].filter((F.col('channel_id').isin(['1', '10', '11']))).display()



# COMMAND ----------

# Fuções when() + otherwise()

# F.when() --> Deve ser utilizada apenas uma vez, após isso apenas .when() e .otherwise(), pois ele traz a 'F.' internamente 

# channel_name --> Nova coluna criada 

df_orders_filter \
    .withColumn('channel_name', F.when(F.col('channel_id') == '1', 'APP')
                .when(F.col('channel_id') == '10', 'SITE') \
                .otherwise('MARKET PLACE')
               ) \
.display()

# COMMAND ----------

# Juntando filter() + withColumn() + when() + otherwise()



df_all = df_orders[['order_id', 'store_id', 'order_amount', 'channel_id']].filter((F.col('channel_id').isin(['1', '10', '11'])))\
    .withColumn('channel_name', F.when(F.col('channel_id') == '1', 'APP')\
    .when(F.col('channel_id') == '10', 'SITE')\
    .otherwise('MARKET PLACE'))\
.display()


# COMMAND ----------

'''

USANDO SQL:


SELECT DISTINCT(channel_id) FROM bronze.orders;


SELECT
  order_id,
  store_id,
  order_amount,
  (
    CASE
      WHEN channel_id = "1" THEN "APP"
      WHEN channel_id = "10" THEN "SITE"
      ELSE "MARKET PLACE"
    END
  ) as channel_name
  FROM
  bronze.orders
WHERE
  channel_id IN ("1", "10", "11");



'''

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 10 - Tratando Dados Ausentes 

# COMMAND ----------

# Importando bibliotecas

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# Verificando as colunas da tabela 'bronze.orders'

df_orders = spark.sql("SELECT * FROM bronze.orders")

df_orders.display()

# COMMAND ----------

# Alterando o tipo de dados da coluna 'order_delivery_cost'

df_orders_filter = df_orders[['order_id', 'order_delivery_cost']].withColumn('order_delivery_cost',F.col('order_delivery_cost').cast(DoubleType()))

df_orders_filter.display()

# COMMAND ----------

# Agrupando os dados da coluna 'order_delivery_cost' para tratar posteriormente os dados null

# Agregando e Agrupando pela média dos valores da coluna 'order_delivery_cost'


df_orders_filter.groupBy().agg({'order_delivery_cost':'mean'}).display()

# COMMAND ----------

# Função collect()

# Usado após filter() e groupBy()

# Usando collect para pegar o valor agregado da média para gravar em uma variavel posterior

# https://sparkbyexamples.com/pyspark/pyspark-collect/

df_orders_filter.groupBy().agg({'order_delivery_cost':'mean'}).collect() # Out: [Row(avg(order_delivery_cost)=7.285979661485239)]

# COMMAND ----------

# OBS: collect()[linha][coluna]

# OBS: O nome da coluna é avg(order_delivery_cost) não mean()

df_orders_filter.groupBy().agg({'order_delivery_cost':'mean'}).collect()[0]['avg(order_delivery_cost)'] # Out: 7.285979661485239

# COMMAND ----------

# Colocando dentro de uma variavel

# Fazendo processo arredondamento --> round() PYTHON

'''

ROUND PYSPARK:

Se a escala for positiva, como scale=2, os valores serão arredondados para o segundo decimal mais próximo. 
Se a escala for negativa, como scale=-1, os valores serão arredondados para o décimo mais próximo. 
Por padrão, scale=0, ou seja, os valores são arredondados para o inteiro mais próximo.

https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.round.html#pyspark.sql.functions.round

https://www.skytowner.com/explore/pyspark_sql_functions_round_method


ROUND PYTHON:

round(number, digits)

Por padrão, digits=0

'''

mean_order_delivery_cost = df_orders_filter.groupBy().agg({'order_delivery_cost':'mean'}).collect()[0]['avg(order_delivery_cost)']
mean_order_delivery_cost = round(mean_order_delivery_cost, 2)
mean_order_delivery_cost # Out: 7.29

# COMMAND ----------

# Usando when() + otherwise() + isNull() + lit()

# lit() Atribuir um valor literal ou constante
 
# https://sparkbyexamples.com/pyspark/pyspark-lit-add-literal-constant/


df_orders_filter.withColumn('order_delivery_cost', F.when(F.col('order_delivery_cost').isNull(), F.lit(mean_order_delivery_cost))\
    .otherwise(F.col('order_delivery_cost'))).display()

# COMMAND ----------

'''

SQL VS PYSPARK:

sql

%sql

-- Alterando valores nulos com Tendência Central
-- Podemos usar uma tendência central, como Media (Menos robusta para Anomalias), Mediana (Mais robusta para Anomalias), Moda 
-- ROUND(number, decimals, operation)


SELECT
  order_id,
  (
    CASE
      WHEN 
        order_delivery_cost IS NULL THEN ROUND((SELECT MEDIAN(CAST(order_delivery_cost AS FLOAT)) FROM bronze.orders),2)
      ELSE order_delivery_cost
    END
  ) AS order_delivery_cost
FROM
  bronze.orders
LIMIT 5;


SELECT
  order_id,
  order_amount,
  (
    CASE
      WHEN 
        order_delivery_cost IS NULL AND order_amount > 0 THEN ROUND((SELECT MEDIAN(CAST(order_delivery_cost AS FLOAT)) FROM bronze.orders),2)
      ELSE order_delivery_cost
    END
  ) AS order_delivery_cost
FROM
  bronze.orders
LIMIT 5;



PYSPARK:

mean_order_delivery_cost = df_order_filter.groupBy().agg({'order_delivery_cost':'mean'}).collect()[0]['avg(order_delivery_cost)']
mean_order_delivery_cost = round(mean_order_delivery_cost, 2)
mean_order_delivery_cost # Out: 7.29




df_order_filter.withColumn('order_delivery_cost', F.when(F.col('order_delivery_cost').isNull(), F.lit(mean_order_delivery_cost))\
    .otherwise(F.col('order_delivery_cost'))).display()


'''

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 11 - Criando Tabela Final Tratada

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

df_orders = spark.sql("SELECT * FROM bronze.orders")
df_orders.display()

# COMMAND ----------

df_orders_estrutura = df_orders[['order_id', 'store_id', 'order_amount', 'order_moment_created']] \
    .withColumn('order_id', F.col('order_id').cast(IntegerType())) \
    .withColumn('store_id', F.col('store_id').cast(IntegerType())) \
    .withColumn('order_amount', F.col('order_amount').cast(DoubleType())) \
    .withColumn('order_moment_created', F.to_date(F.regexp_replace(F.substring(F.col('order_moment_created'), 1, 9), ' ', ''), 'M/d/yyyy'))

# COMMAND ----------

# isNull() x isNotNull() == ~isNull() --> PYSPARK

# isin() x ~isin() == Not isin() --> PYSPARK

# is null x is not null --> SQL
  
# OBS: ~ --> É negação

df_orders_filter = df_orders_estrutura.filter(
    (F.col('order_amount') < 10000) &
    (~F.col('order_moment_created').isNull())
)

# COMMAND ----------

# Deletar dados duplicados coalesce(1) + dropDuplicates()

df_orders_filter = df_orders_filter.coalesce(1).dropDuplicates()
df_orders_filter.display()

# COMMAND ----------

# Ver quantidade de linhas 

display(df_orders_filter.count())

# COMMAND ----------

# Criando um dataframe através da tabela bronze.stores

df_stores = spark.sql("SELECT store_id, store_name FROM bronze.stores")
df_stores.display()

# COMMAND ----------

# Tratado 'store_id' da tabela 'bronze.store' que agora está dentro de um dataframe 'df_stores'

df_stores_estrura = df_stores.withColumn('store_id', F.col('store_id').cast(IntegerType()))

# COMMAND ----------

# Deletando dados duplicados

df_stores_estrura = df_stores_estrura.coalesce(1).dropDuplicates()

# COMMAND ----------

# Juntando os dataframes 'df_orders_filter' + 'df_stores_estrutura'

# OBS: Para fazer join as colunas tem de ser do mesmo tipo de dado

# Coluna de conexão é 'stored_id' (Foreignkey )

df_orders_stores = df_orders_filter.join(df_stores_estrura, on=['store_id'], how='inner')

# COMMAND ----------

# Agrupando e Agregando (Função sum (soma)) para criar um novo datrafame 

# Novo 'df_orders_stores_total_amount' tratado, agrupado e agregado que será de base para a criação de uma nova tabela trata 

# IMPORTANTE: O order_id não é passado no groupBy() devido ao fato que ele faz uma união a nível de linha e assim não trara os dados corretamente que queremos

df_orders_stores_total_amount = df_orders_stores \
    .groupBy(['order_moment_created', 'store_id', 'store_name'])\
    .agg({'order_amount':'sum'}) \
    .withColumnRenamed('sum(order_amount)', 'total_amount')

# COMMAND ----------

# Criando a tabela tratada no diretório 'prata'

# OBS: Depdende de como for trabalhar no dia-a-dia podemos usar também as opções option("mergeSchema", "true"), mode('append)

df_orders_stores_total_amount.write \
    .mode('overwrite') \
    .saveAsTable('prata.orders_store_total_amount')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Fazendo select na nova trabela tratada 'orders_store_total_amount' na base de dados prata
# MAGIC
# MAGIC SELECT * FROM prata.orders_store_total_amount;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) FROM prata.orders_store_total_amount;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Exemplo de como dropar tabela 
# MAGIC
# MAGIC -- DROP TABLE prata.orders_store_total_amount;
