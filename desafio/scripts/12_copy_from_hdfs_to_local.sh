#! /bin/bash

# Copia do HDFS para o diretório local

# Carregando configuração geral 
source <(grep = config.ini)

echo "Rodando scrips python em ${DATE}..."
docker exec jupyter-spark /opt/spark-2.4.1-bin-without-hadoop/bin/spark-submit /desafio/scripts/process/process.py

echo "Removendo arquivos existentes"
rm ${BASE_DIR}/desafio/*.zip
rm ${BASE_DIR}/desafio/gold/*.csv

echo "Iniciando cópia de HDFS para diretório local [${HDFS_LOCAL_GOLD_DIR}] "
for entidade in "${ENTIDADES_DIM[@]}"
do
    echo "Copiando /datalake/desafio/gold/dim_${entidade}/dim_${entidade}.csv"

    docker exec namenode hadoop fs -copyToLocal /datalake/desafio/gold/${entidade}/${entidade}.csv /desafio/gold
    
    # exportação para zipfile
    zip ${BASE_DIR}/desafio/zip_temp/${entidade}.zip ${BASE_DIR}/desafio/gold/${entidade}.csv
    
    echo "---------------  FIM ${entidade} -------------------------"
done

echo "Cópia finalizada..."