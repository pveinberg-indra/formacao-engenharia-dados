#! /bin/bash

# Carregando configuração geral 
source <(grep = config.ini)

FROM=${HDFS_LOCAL_DIR}
RAW=${HDFS_BASE_DIR}${HDFS_LOCAL_DIR}

echo "Iniciando transferência de arquivos de [${FROM}] para HDFS [${RAW}] em ${DATE}..."

echo "Estrutura criada em ${HDFS_BASE_DIR}"
docker exec namenode hdfs dfs -ls ${HDFS_BASE_DIR}

echo "Copiando os arquivos da borda para o servidor HDFS..."
for entidade in "${ENTIDADES[@]}"
do
    echo "Criando diretório da entidade [${entidade}]"
    docker exec namenode hdfs dfs -mkdir -p ${RAW}/${entidade}

    echo "Copiando [${entidade}]"
    docker exec namenode hdfs dfs -copyFromLocal ${FROM}/${entidade}.csv ${RAW}/${entidade}
done

echo "Operação finalizada, arquivos disponíveis em  ${RAW}/"
docker exec namenode hdfs dfs -ls ${RAW}