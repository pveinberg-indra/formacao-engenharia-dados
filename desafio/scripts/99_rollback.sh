#! /bin/bash

# Carregando configuração geral 
source <(grep = config.ini)

echo "Iniciando rollback em ${DATE}..."


echo "removing ${BASE_DIR}"

echo "removing docker exec namenode hdfs dfs -rm -r -f ${HDFS_BASE_DIR}${HDFS_LOCAL_DIR}"
echo "removing docker exec namenode hdfs dfs -rm -r -f ${HDFS_BASE_DIR}${HDFS_LOCAL_GOLD_DIR}"


