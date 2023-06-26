#! /bin/bash

# Copia do HDFS para o diretório local

# Carregando configuração geral 
source <(grep = config.ini)

echo "Iniciando cópia de HDFS para diretório local [${HDFS_LOCAL_GOLD_DIR}] em ${DATE}..."

for entidade in "${ENTIDADES_DIM[@]}"
do
    echo "Copiando /datalake/desafio/gold/dim_${entidade}/dim_${entidade}.csv"
    docker exec namenode hadoop fs -copyToLocal /datalake/desafio/gold/${entidade}/${entidade}.csv /desafio/gold
    echo "---------------  FIM ${entidade} -------------------------"
done

echo "Cópia finalizada..."