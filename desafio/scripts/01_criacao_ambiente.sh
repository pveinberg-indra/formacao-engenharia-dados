#! /bin/bash

# Carregando configuração geral 
source <(grep = config.ini)

echo "Iniciando criação de diretórios no ambiente em ${DATE}..."

# Criação diretório raíz
mkdir -p ${BASE_DIR}

# Criação diretório com scripts e hql para DLLs das tabelas Hive
mkdir -p ${BASE_DIR}${HQL_DIR}

# Criação do diretório de logs
mkdir -p ${BASE_DIR}${LOG_DIR}

# Criação dos diretórios run e app
mkdir -p ${BASE_DIR}${RUN_DIR}
mkdir -p ${BASE_DIR}${APP_DIR}

# Criação de deretórios raw e gold (edge node)
mkdir -p ${BASE_DIR}${HDFS_LOCAL_DIR}
mkdir -p ${BASE_DIR}${HDFS_LOCAL_GOLD_DIR}

# Criação da estrutura no HDFS (namenode)
docker exec namenode hdfs dfs -mkdir -p ${HDFS_BASE_DIR}${HDFS_LOCAL_DIR}
docker exec namenode hdfs dfs -mkdir -p ${HDFS_BASE_DIR}${HDFS_LOCAL_GOLD_DIR}

echo "Finalizado com sucesso"