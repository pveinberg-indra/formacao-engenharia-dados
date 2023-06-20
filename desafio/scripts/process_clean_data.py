#!/usr/bin/env python

# Tratamento dos dados utilizando Pyspark

# Importação de libs
from pyspark.sql import SparkSession, dataframe
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType, DateType
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql import functions as f
from configparser import ConfigParser
import logging
import os
import math
import re

# Lendo arquivo .ini com as configurações do projeto
# da pasta local das máquinas docker
BASE_CONFIGS = " /desafio/scripts/config.ini"
config = ConfigParser()
config.read(BASE_CONFIGS)

# Setando variáveis locais
DB_EXT = config['HIVE']['DB_EXT'].lower()
DB_STG = config['HIVE']['DB_STG'].lower()
entidades = config['EDGE']['ENTIDADES'].replace(
    "(", "").replace(")", "").replace('"', '').split(' ')
HDFS_GOLD_DIR = config['HDFS']['HDFS_LOCAL_GOLD_DIR']
LOG_DIR = config['GERAL']['LOG_DIR']

# Configuração de arquivo de log
logging.basicConfig(filename = LOG_DIR + "/log_desafio.log", \
                    format="%(asctime)s %(name)s %(levelname)s %(message)s", \
                    level=logging.INFO)


logging.info("Iniciando aplicação")

# Definição de funções

# Salvando dataframes no HDFS


def save_df(_name, _df, _dir):
    output = "{_dir}/{_name}"
    erase = "hdfs dfs -rm {output}/*"
    rename = "hdfs dfs -mv {output}/part-*.csv {output}/{_name}.csv"

    try:
        logging.info(erase)
        os.system(erase)

        logging.info("Transferindo dados: {output}")
        _df.coalesce(1).write\
            .format("csv")\
            .option("header", True)\
            .option("delimiter", ";")\
            .mode("overwrite")\
            .save("{output}/")

        logging.info(rename)
        os.system(rename)

        logging.info("{_name} persistida com sucesso em {output}\n")
    except Exception as e:
        logging.error("Erro: {e}")
        pass

# Processando dataframes
# Alterando valores em branco ou nulos para "Não informado"
# Configurando os tipos de dados corretos
# Configurando valores faltantes para zero


def process_dataframe_rdd(_dataframe: dataframe.DataFrame, _datatypes: dict) -> dataframe.DataFrame:
    clean_string_udf = udf(lambda x: clean_empty(x), StringType())
    clean_zero_udf = udf(lambda x: "0.0" if x == "" else x)
    cast_float_udf = udf(lambda x: float(
        str(x).strip().replace('.', '').replace(',', '.')))
    mapping_types = {
        'float': DoubleType(),
        'object': StringType(),
        'datetime64[ns]': DateType()
    }
    for field, _type in _datatypes.items():
        if _type == 'float':
            _dataframe = _dataframe.withColumn(
                field, clean_zero_udf(col(field)))
            _dataframe = _dataframe.withColumn(
                field, cast_float_udf(col(field)))

        if _type == 'object':
            _dataframe = _dataframe.withColumn(
                field, clean_string_udf(col(field)))

        _dataframe = _dataframe.withColumn(
            field, col(field).cast(mapping_types[_type]))

    return _dataframe


# Limpeza de campos string
def clean_empty(_txt: str) -> str:
    if _txt == "":
        return "Não informado"

    rs = re.search("\s{2,}", _txt)

    if rs != None:

        if rs.span()[0] == 0:
            return "Não informado"
        _txt = _txt.replace(rs.group(), ' ')

    return _txt


# Testes unitários para a função clean_empty
assert clean_empty("") == "Não informado", "Precisa retornar 'Não informado'"
assert clean_empty("    ") == "Não informado"
assert clean_empty("New York") == "New York"
assert clean_empty("New  York") == "New York"

# Iniciando SparkSession
spark = SparkSession.builder.master("local[*]") \
    .enableHiveSupport() \
    .getOrCreate()

# Configruação dos tipos das tabelas
# O dict a seguir informa, para cada entidade, os campos e tipos de dados
_types = {
    # regiao
    'regiao': {'region_code': 'object',
               'region_name': 'object',
               'dt_foto': 'datetime64[ns]'},
    # divisao
    'divisao': {'division': 'object',
                'division_name': 'object',
                'dt_foto': 'datetime64[ns]'},
    # vendas
    'vendas': {'actual_delivery_date': 'datetime64[ns]',
               'customerkey': 'object',
               'datekey': 'datetime64[ns]',
               'discount_amount': 'float',
               'invoice_date': 'datetime64[ns]',
               'invoice_number': 'object',
               'item_class': 'object',
               'item_number': 'object',
               'item': 'object',
               'line_number': 'object',
               'list_price': 'float',
               'order_number': 'object',
               'promised_delivery_date': 'datetime64[ns]',
               'sales_amount': 'float',
               'sales_amount_based_on_list_price': 'float',
               'sales_cost_amount': 'float',
               'sales_margin_amount': 'float',
               'sales_price': 'float',
               'sales_quantity': 'float',
               'sales_rep': 'float',
               'u_m': 'object',
               'dt_foto': 'datetime64[ns]'},
    # endereco
    'endereco': {'address_number': 'object',
                 'city': 'object',
                 'country': 'object',
                 'customer_address_1': 'object',
                 'customer_address_2': 'object',
                 'customer_address_3': 'object',
                 'customer_address_4': 'object',
                 'state': 'object',
                 'zip_code': 'object',
                 'dt_foto': 'datetime64[ns]'},
    # clientes
    'clientes': {'address_number': 'object',
                 'business_family': 'object',
                 'business_unit': 'object',
                 'customer': 'object',
                 'customerkey': 'object',
                 'customer_type': 'object',
                 'division': 'object',
                 'line_of_business': 'object',
                 'phone': 'object',
                 'region_code': 'object',
                 'regional_sales_mgr': 'object',
                 'search_type': 'object',
                 'dt_foto': 'datetime64[ns]'}}


# Início do processo de processamento e persistência no HDFS
logging.info("Iniciando persistência das entidades para HDFS")
for entidade in entidades:
    data = spark.sql("select * from {DB_STG}.tbl_{entidade}")
    data = process_dataframe_rdd(data, _types[entidade])
    logging.info("Exportando {HDFS_GOLD_DIR}/{entidade}/{entidade}.csv")
    save_df(_df=data, _dir=HDFS_GOLD_DIR, _name=entidade)
    logging.info("Exportação de tabelas realizada com sucesso")
