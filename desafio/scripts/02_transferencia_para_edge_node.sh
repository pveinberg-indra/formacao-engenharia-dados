#! /bin/bash

# Carregando configuração geral 
source <(grep = config.ini)

echo "Iniciando transferência de arquivos em ${DATE}..."

FROM=${BASE_DIR}${EDGE_BASE_DIR}
TO=${BASE_DIR}${HDFS_LOCAL_DIR}

echo $FROM
echo $TO

echo "Criando diretório de entrada [RAW]..."
mkdir -p $TO

echo "Habilitando permissões..."
chmod 775 -R ${FROM}/*
chmod 775 -R ${TO}/*

echo "Copiando os arquivos da fonte para o servidor de borda [${FROM}]..."
for entidade in "${ENTIDADES[@]}"
do
    echo "Copiando [${entidade}]"
    cp ${FROM}/${entidade^^}.csv ${TO}/${entidade}.csv
done

echo "Operação finalizada, arquivos disponíveis em  ${TO}"

ls -lat ${TO}