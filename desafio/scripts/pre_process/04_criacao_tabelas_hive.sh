#! /bin/bash

# Carregando configuração geral 
source <(grep = ../config.ini)

echo "Iniciando criação de tabelas no Hive em ${DATE}..."

echo "Criando arquivos DLL para posterior criação de tabelas..." 
python3 05_create_table_DLLs.py

echo "Criando tabelas no HIVE..."

for entidade in "${ENTIDADES[@]}"
do
    echo "Criando tbl_${entidade}"
    docker exec hive-server beeline -u jdbc:hive2://${BEELINE_HOST}:${BEELINE_PORT} -f ${HQL_DIR}/create_table_${entidade}.hql
    echo "---------------  FIM ${entidade} -------------------------"
done

echo "Criação finalizada..."