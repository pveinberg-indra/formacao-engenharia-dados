#!/usr/bin/env python

# Imports
from pyspark.sql import SparkSession, dataframe
from pyspark.sql import HiveContext
from pyspark.sql.functions import col, regexp_replace, udf, year, month, quarter, sha2, concat_ws, when
from pyspark.sql.types import DateType
from configparser import ConfigParser
import logging
import os
from datetime import datetime, timedelta

# SET INIT FILE
BASE_CONFIGS = "/desafio/scripts/config.ini"
config = ConfigParser()
config.read(BASE_CONFIGS)

# SET VARS
DB_STG = config['HIVE']['DB_STG'].lower()
entidades = config['EDGE']['ENTIDADES'].replace("(", "").replace(")", "").replace('"', '').split(' ')
HDFS_GOLD_DIR = config['HDFS']['HDFS_BASE_DIR'] + config['HDFS']['HDFS_LOCAL_GOLD_DIR']
LOG_DIR = config['GERAL']['LOG_DIR']

# D-1
D1 = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

# CONFIG LOGGING
logging.basicConfig(filename="{}/log_desafio.log".format(LOG_DIR),
                    format="%(asctime)s %(name)s %(levelname)s %(message)s",
                    level=logging.INFO)

logging.info("Iniciando transferência de dados")

# FUNÇÕES
# Save dataframe 
def save_df(_name, _df, _dir):
    output = "{}/{}".format(_dir, _name)
    erase = "hdfs dfs -rm {}/*".format(output)
    rename = "hdfs dfs -mv {}/part-*.csv {}/{}.csv".format(output, output, _name)

    try:
        logging.info(erase)
        os.system(erase)

        logging.info("Transferindo dados: {}".format(output))
        _df.coalesce(1).write\
            .format("csv")\
            .option("header", True)\
            .option("delimiter", ";")\
            .mode("overwrite")\
            .save("{}/".format(output))

        logging.info(rename)
        os.system(rename)

        logging.info("{} persistida com sucesso em {}\n".format(_name, output))
    except Exception as e:
        logging.error("Erro: {}".format(e))
        pass

def cast_date(_date_str):
    if _date_str != None: 
        try:
            date_time_obj = datetime. strptime(_date_str, '%d/%m/%Y')
            return date_time_obj
        except:
            logging.error("Houve um erro converindo uma data: {}".format(_date_str))
            pass


# FIM DECLARAÇÃO DE FUNÇÕES

# Iniciando spark
spark = SparkSession.builder.master("local[*]")\
    .enableHiveSupport()\
    .getOrCreate()
    

stage_sql = """select 
    tv.customerkey,
    tv.discount_amount,
    tv.invoice_date,
    tv.list_price,
    tv.sales_amount,
    tv.sales_amount_based_on_list_price,
    tv.sales_cost_amount,
    tv.sales_margin_amount,
    tv.sales_price,
    tv.sales_quantity,
    tv.dt_foto as data_foto,
    tc.address_number,
    tc.customer,
    te.city,
    te.country,
    te.state
    from vendas tv 
        LEFT JOIN clientes tc on tc.customerkey = tv.customerkey 
        LEFT JOIN endereco te on te.address_number = tc.address_number 
        LEFT JOIN divisao td on td.division = tc.division 
        LEFT JOIN regiao tr on tr.region_code = tc.region_code 
"""

# Carga dos dataframes

dataframes = {}
for entidade in entidades:
    dataframes[entidade] = spark.sql("select * from {}.tbl_{}".format(DB_STG, entidade))
    dataframes[entidade].createOrReplaceTempView(entidade)

df_stage = spark.sql(stage_sql)
del dataframes


# Cast campos data
campos_data = ['invoice_date']
cast_date_udf =  udf(lambda x: cast_date(x), DateType())  

for data in campos_data:
    df_stage = df_stage.withColumn(data, cast_date_udf(col(data)).alias(data))

    
# Add campos para dimensao tempo (ano, mês, trimestre)
df_stage = df_stage \
    .withColumn('_year', year(col(campos_data[0]))) \
    .withColumn('_month', month(col(campos_data[0]))) \
    .withColumn('_quarter', quarter(col(campos_data[0])))

# criar chaves
df_stage = df_stage.withColumn('DW_VENDAS', sha2(concat_ws("", df_stage.customerkey, df_stage.invoice_date, df_stage.sales_price), 256))
df_stage = df_stage.withColumn('DW_CLIENTES', sha2(concat_ws("", df_stage.customerkey, df_stage.customer), 256))
df_stage = df_stage.withColumn('DW_TEMPO', sha2(concat_ws("", df_stage.invoice_date), 256))
df_stage = df_stage.withColumn('DW_LOCALIDADE', sha2(concat_ws("", df_stage.customerkey, df_stage.address_number), 256))

# tratar campos em branco ou nulos
ni = "Não Informado"

campos_string = ['address_number', 'city', 'state', 'country', 'customer']

for c in campos_string:
    df_stage = df_stage.withColumn(c, regexp_replace(col(c), '\s{2,}', ni)) \
        .withColumn(c, when(col(c).isNull(), ni).otherwise(col(c))) 
    
# Tratamento dos campos numéricos (nulos p zero e formato ok)
campos_num = ['list_price', 'sales_amount', 'sales_amount_based_on_list_price', \
              'sales_cost_amount', 'sales_margin_amount', 'sales_price']

for c in campos_num:
    df_stagea = df_stage.withColumn(c, regexp_replace(col(c), ",", "")) \
        .withColumn(c, regexp_replace(col(c), ",", ".")) \
        .withColumn(c, col(c).cast("float"))
    
df_stage.createOrReplaceTempView('stage')

df_stage.select(campos_num).collect()

# fim declaracao stage

dir_teste = '/datalake/teste_final'

# DIM_LOCALIDADE
dim_localidade = spark.sql('''
    SELECT DISTINCT DW_LOCALIDADE,
        address_number,
        city,
        state,
        country
    from stage
''')

# DIM_TEMPO
dim_tempo = spark.sql('''
    SELECT DISTINCT DW_TEMPO, 
        invoice_date,        
        _year,
        _month,
        _quarter
    from stage
''')

# DIM_CLIENTES
dim_clientes = spark.sql('''SELECT DISTINCT DW_CLIENTES, 
    customerkey, 
    customer 
        FROM stage''')

# FT_VENDAS
ft_vendas = spark.sql('''
    SELECT DISTINCT DW_VENDAS,
        list_price, 
        sales_amount, 
        sales_amount_based_on_list_price, 
        sales_cost_amount,
        sales_margin_amount,
        sales_price
    FROM stage
''')

save_df(_df=dim_tempo, _name='dim_tempo', _dir=dir_teste)
save_df(_df=dim_clientes, _name='dim_clientes', _dir=dir_teste)
save_df(_df=dim_localidade, _name='dim_localidade', _dir=dir_teste)
save_df(_df=ft_vendas, _name='ft_vendas', _dir=dir_teste)

logging.info("Processo finalizado.")