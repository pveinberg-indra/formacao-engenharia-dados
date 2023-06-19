#! /bin/bash

# Carregando configuração geral 
source <(grep = config.ini)

FROM=${HDFS_LOCAL_DIR}

echo "Iniciando transferência de arquivos de [${FROM}] para HDFS [${HDFS_BASE_DIR}] em ${DATE}..."

echo "Criando filesystem no HDFS..."

docker exec namenode hdfs dfs -mkdir -p ${HDFS_BASE_DIR}/raw
docker exec namenode hdfs dfs -mkdir -p ${HDFS_BASE_DIR}/gold

echo "Estrutura criada em ${HDFS_BASE_DIR}"
docker exec namenode hdfs dfs -ls ${HDFS_BASE_DIR}

echo "Copiando os arquivos da borda para o servidor HDFS..."
for entidade in "${ENTIDADES[@]}"
do
    echo "Criando diretório da entidade [${entidade}]"
    docker exec namenode hdfs dfs -mkdir -p ${HDFS_BASE_DIR}/raw/${entidade}

    echo "Copiando [${entidade}]"
    docker exec namenode hdfs dfs -copyFromLocal ${FROM}/${entidade}.csv ${HDFS_BASE_DIR}/raw/${entidade}
done

echo "Operação finalizada, arquivos disponíveis em  ${HDFS_BASE_DIR}/raw/"
docker exec namenode hdfs dfs -ls ${HDFS_BASE_DIR}/raw/