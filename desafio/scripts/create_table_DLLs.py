#!/usr/bin/env python

# Imports
import pandas as pd
import os
from datetime import datetime, timedelta
from configparser import ConfigParser

# Config files
config = ConfigParser()
config.read('./config.ini')
diretorio_arquivos = os.path.expanduser(f"{config['GERAL']['BASE_DIR']}{config['EDGE']['EDGE_BASE_DIR']}") 
destino_hqls = os.path.expanduser(f"{config['GERAL']['BASE_DIR']}{config['GERAL']['HQL_DIR']}") 

# Reads files in source folder
files = os.listdir(diretorio_arquivos)
files

# Read metadata
metadados = []
for file in files:
    data = pd.read_csv(f"{diretorio_arquivos}/{file}", sep=';')
    campos_tabela = [f"{column.lower().replace(' ', '_').replace('/','_')} string" for column in list(data.columns)]
    metadados.append(
        {'tabela': file.split('.')[0].lower(), 
         'campos': campos_tabela})

# Set DLLs
DB_EXT=config['HIVE']['DB_EXT'].lower()
DB_STG=config['HIVE']['DB_STG'].lower()
HDFS_BASE_DIR = config['HDFS']['HDFS_BASE_DIR']
HDFS_RAW_DIR= config['HDFS']['HDFS_BASE_DIR'] + config['HDFS']['HDFS_LOCAL_DIR']
HDFS_GLD_DIR= config['HDFS']['HDFS_BASE_DIR'] + config['HDFS']['HDFS_LOCAL_GOLD_DIR']
PARTICAO=(datetime.today() - timedelta(1)).strftime('%Y%m%d')

for tbl in metadados:
    campos = ",\n\t".join(tbl['campos'])
    create=f"""-- TABELA {tbl['tabela']} on Hive

    CREATE DATABASE IF NOT EXISTS {DB_EXT}; 
    CREATE DATABASE IF NOT EXISTS {DB_STG};

    DROP TABLE {DB_EXT}.tbl_{tbl['tabela']};

    CREATE EXTERNAL TABLE IF NOT EXISTS {DB_EXT}.tbl_{tbl['tabela']} (
        {campos}
    )
    COMMENT "Tabela de {tbl['tabela']}"
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ";"
    STORED AS TEXTFILE
    location  "{HDFS_RAW_DIR}/{tbl['tabela']}"
    TBLPROPERTIES ("skip.header.line.count"="1");

    SELECT * FROM {DB_EXT}.tbl_{tbl['tabela']} LIMIT 5;

    -- Tabela {tbl['tabela']} particionada

    DROP TABLE {DB_STG}.tbl_{tbl['tabela']};

    CREATE TABLE IF NOT EXISTS {DB_STG}.tbl_{tbl['tabela']} (
        {campos}
    )
    PARTITIONED BY (DT_FOTO STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
    STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
    TBLPROPERTIES ('orc.compress'='SNAPPY');

    SET hive.exec.dynamic.partition=true;
    SET hive.exec.dynamic.partition.mode=nonstrict;

    INSERT OVERWRITE TABLE 
        {DB_STG}.tbl_{tbl['tabela']}
    PARTITION(DT_FOTO)
    SELECT
        {campos},
        '{PARTICAO}' as DT_FOTO
    FROM {DB_EXT}.tbl_{tbl['tabela']};

    SELECT * FROM {DB_STG}.tbl_{tbl['tabela']} LIMIT 5;

    -- --------------------------------------------------------------
    --  FIM DO SCRIPT {tbl['tabela']}
    -- --------------------------------------------------------------

    """
    destino = f"{destino_hqls}/create_table_{tbl['tabela']}.hql"
    
    file_hql = open(destino, 'w')
    file_hql.write(create)
    file_hql.close()

