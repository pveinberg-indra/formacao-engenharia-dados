# coding: utf-8

# ## Cleanning data using Pyspark

# Imports
from pyspark.sql import SparkSession, dataframe
# https://sparkbyexamples.com/pyspark/pyspark-sql-types-datatype-with-examples/?expand_article=1
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType, DateType
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql import functions as f
from configparser import ConfigParser
import logging
import os
import math
import re
from datetime import datetime, timedelta

# SET INIT FILE
BASE_CONFIGS = "/desafio/scripts/config.ini"
config = ConfigParser()
config.read(BASE_CONFIGS)

# SET VARS
DB_EXT = config['HIVE']['DB_EXT'].lower()
DB_STG = config['HIVE']['DB_STG'].lower()
entidades = config['EDGE']['ENTIDADES'].replace("(", "").replace(")", "").replace('"', '').split(' ')
HDFS_GOLD_DIR = config['HDFS']['HDFS_BASE_DIR'] + config['HDFS']['HDFS_LOCAL_GOLD_DIR']
LOG_DIR = config['GERAL']['LOG_DIR']

# D-1
D1 = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

# CONFIG LOGGING
logging.basicConfig(filename=f"{LOG_DIR}/log_desafio.log",
                    format="%(asctime)s %(name)s %(levelname)s %(message)s",
                    level=logging.INFO)

logging.info("Iniciando aplicação")


# Save dataframe 
def save_df(_name, _df, _dir):
    output = f"{_dir}/{_name}"
    erase = f"hdfs dfs -rm {output}/*"
    rename = f"hdfs dfs -mv {output}/part-*.csv {output}/{_name}.csv"
    
    try:
        logging.info(erase)
        os.system(erase)

        logging.info(f"Transferindo dados: {output}")
        _df.coalesce(1).write            .format("csv")            .option("header", True)            .option("delimiter", ";")            .mode("overwrite")            .save(f"{output}/")
        
        logging.info(rename)
        os.system(rename)
        
        logging.info(f"{_name} persistida com sucesso em {output}\n")
    except Exception as e:
#         logging.error(f"Erro: {e}")
        logging.error("Não foi possível persistir")
        pass

# Process dataframes
def process_dataframe_rdd(_dataframe:dataframe.DataFrame, _datatypes:dict) -> dataframe.DataFrame:
    clean_string_udf = udf(lambda x: clean_empty(x), StringType())
    clean_zero_udf = udf(lambda x : "0.0" if x == "" else x)
    cast_float_udf = udf(lambda x: float(str(x).strip().replace('.', '').replace(',', '.')))
    mapping_types  = {
        'float': DoubleType(),
        'object': StringType(), 
        'datetime64[ns]': DateType()
    }
    for field, _type in _datatypes.items():
        if _type == 'float':
            _dataframe = _dataframe.withColumn(field, clean_zero_udf(col(field)))
            _dataframe = _dataframe.withColumn(field, cast_float_udf(col(field)))
            
        if _type == 'object':
            _dataframe = _dataframe.withColumn(field, clean_string_udf(col(field)))
        
        if field == 'dt_foto':
            _dataframe = _dataframe.fillna(D1, subset=['dt_foto'])
            
        _dataframe = _dataframe.withColumn(field, col(field).cast(mapping_types[_type]))        
        
    return _dataframe

def cast_date(_date_str):
    try:
        date_time_obj = datetime. strptime(_date_str, '%d/%m/%Y')
        return date_time_obj
    except:
        pass
    
    
# Process dataframes
def process_dataframe(_dataframe:dataframe.DataFrame, _datatypes:dict) -> dataframe.DataFrame:
    clean_string_udf = udf(lambda x: clean_empty(x), StringType())
    clean_zero_udf = udf(lambda x : "0.0" if x == "" else x)
    cast_float_udf = udf(lambda x: float(str(x).strip().replace('.', '').replace(',', '.')))
    cast_stringdate_to_date_udf = udf(lambda x: cast_date(x), DateType())

    mapping_types  = {
        'float': DoubleType(),
        'object': StringType(), 
        'datetime64[ns]': DateType()
    }
    
    for field, _type in _datatypes.items():
        if _type == 'float':
            _dataframe = _dataframe.withColumn(field, clean_zero_udf(col(field)))
            _dataframe = _dataframe.withColumn(field, cast_float_udf(col(field)))
            
        if _type == 'object':
            _dataframe = _dataframe.withColumn(field, clean_string_udf(col(field)))
        
        if _type == 'datetime64[ns]' and field != 'dt_foto':
            _dataframe = _dataframe.withColumn(field,                                                cast_stringdate_to_date_udf(col(field)))
            
        _dataframe = _dataframe.withColumn(field, col(field).cast(mapping_types[_type]))        
        
    return _dataframe

# Limpeza de campos string
def clean_empty(_txt:str) -> str:
    if _txt == "":
        return "Não informado"
    
    rs = re.search("\s{2,}", _txt)
    
    if rs != None:
    
        if rs.span()[0] == 0:
            return "Não informado"
        _txt = _txt.replace(rs.group(), ' ')

    return _txt

assert clean_empty("") == "Não informado", "Precisa retornar 'Não informado'"
assert clean_empty("    ") == "Não informado"
assert clean_empty("New York") == "New York"
assert clean_empty("New  York") == "New York"


# In[4]:


spark = SparkSession.builder.master("local[*]")    .enableHiveSupport()    .getOrCreate()


# In[5]:


# Configruação dos tipos das tabelas
_types = {
# regiao
'regiao': {'region_code': 'object', 
                    'region_name': 'object', 
                    'dt_foto': 'datetime64[ns]'}, 
#divisao
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


# ## DIMENSIONAL

# In[6]:


stage_sql = f"""select 
    tv.actual_delivery_date,
    tv.customerkey,
    tv.datekey,
    tv.discount_amount,
    tv.invoice_date,
    tv.invoice_number,
    tv.item_class,
    tv.item_number,
    tv.item,
    tv.line_number,
    tv.list_price,
    tv.order_number,
    tv.promised_delivery_date,
    tv.sales_amount,
    tv.sales_amount_based_on_list_price,
    tv.sales_cost_amount,
    tv.sales_margin_amount,
    tv.sales_price,
    tv.sales_quantity,
    tv.sales_rep,
    tv.u_m,
    tv.dt_foto as data_foto,
    tc.address_number,
    tc.business_family,
    tc.business_unit,
    tc.customer,
    tc.customer_type,
    tc.division,
    tc.line_of_business,
    tc.phone,
    tc.region_code,
    tc.regional_sales_mgr,
    tc.search_type,
    te.city,
    te.country,
    te.customer_address_1,
    te.customer_address_2,
    te.customer_address_3,
    te.customer_address_4,
    te.state,
    te.zip_code,
    td.division_name,
    tr.region_name
    from vendas tv 
        LEFT JOIN clientes tc on tc.customerkey = tv.customerkey 
        LEFT JOIN endereco te on te.address_number = tc.address_number 
        LEFT JOIN divisao td on td.division = tc.division 
        LEFT JOIN regiao tr on tr.region_code = tc.region_code 
"""


# In[7]:


# Carga dos dataframes
df_vendas = spark.sql("select * from {}.tbl_vendas".format(DB_STG))
df_clientes = spark.sql("select * from {}.tbl_clientes".format(DB_STG))
df_divisao = spark.sql("select * from {}.tbl_divisao".format(DB_STG))
df_endereco = spark.sql("select * from {}.tbl_endereco".format(DB_STG))
df_regiao = spark.sql("select * from {}.tbl_regiao".format(DB_STG))


# In[8]:


# ['clientes', 'divisao', 'endereco', 'regiao', 'vendas']
# Criação das views
df_vendas.createOrReplaceTempView('vendas')
df_clientes.createOrReplaceTempView('clientes')
df_divisao.createOrReplaceTempView('divisao')
df_endereco.createOrReplaceTempView('endereco')
df_regiao.createOrReplaceTempView('regiao')


# In[9]:


df_stage = spark.sql(stage_sql)


# In[10]:


# Configruação dos tipos das tabelas
_fields = {
# regiao
'region_code': 'object', 
'region_name': 'object', 
# 'dt_foto': 'datetime64[ns]', 
#divisao
'division': 'object', 
'division_name': 'object', 
# vendas
'actual_delivery_date': 'datetime64[ns]',
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
'data_foto': 'datetime64[ns]', 
# endereco
'address_number': 'object', 
'city': 'object', 
'country': 'object', 
'customer_address_1': 'object',
'customer_address_2': 'object', 
'customer_address_3': 'object', 
'customer_address_4': 'object',
'state': 'object', 
'zip_code': 'object', 
# clientes
'address_number': 'object',
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
'search_type': 'object'}


# In[11]:


# process stage
# processando o tabelão stage para ter todas as informações consistentes
df_stage = process_dataframe(df_stage, _fields)


# In[12]:


# criar colunas dimensionais
c = 'invoice_date'
df_stage = df_stage     .withColumn('_year', year(col(c)))     .withColumn('_month', month(col(c)))     .withColumn('_quarter', quarter(col(c)))     .withColumn('_weekofyear', weekofyear(col(c)))


# In[13]:


# criar chaves
df_stage = df_stage.withColumn('DW_CLIENTES', sha2(concat_ws("", df_stage.customerkey, df_stage.customer), 256))
df_stage = df_stage.withColumn('DW_TEMPO', sha2(concat_ws("", df_stage.invoice_date, df_stage._year,  df_stage._month,  df_stage._quarter,  df_stage._weekofyear), 256))
df_stage = df_stage.withColumn('DW_VENDAS', sha2(concat_ws("", df_stage.customerkey, df_stage.invoice_number, df_stage.datekey, df_stage.line_number), 256))
df_stage = df_stage.withColumn('DW_LOCALIDADE', sha2(concat_ws("", df_stage.customerkey, df_stage.address_number), 256))

df_stage.createOrReplaceTempView('stage')


# In[14]:


df_stage.printSchema()


# In[15]:


# #1
dim_clientes = df_stage.select('DW_CLIENTES', 'customerkey', 'customer', 'line_of_business', 'business_family', 'business_unit', 'customer_type', 'region_name', 'regional_sales_mgr', 'search_type', 'phone', 'division_name')
# dim_clientes.createOrReplaceTempView('dim_clientes')


# In[16]:


dim_clientes.printSchema()


# In[17]:


save_df(_df=dim_clientes, _dir=HDFS_GOLD_DIR, _name='dim_clientes')


# In[18]:


dim_clientes.printSchema()


# In[19]:


dim_clientes.count()


# In[20]:


df = dim_clientes.toPandas()
df.head()

