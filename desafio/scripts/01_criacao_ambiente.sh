#! /bin/bash

# Carregando configuração geral 
source <(grep = config.ini)

echo "Iniciando criação de diretórios no ambiente em ${DATE}..."

mkdir -p /workspace/formacao-engenharia-dados
mkdir -p /workspace/formacao-engenharia-dados/desafio/scripts/hql
mkdir -p /workspace/formacao-engenharia-dados/desafio/logs

mkdir -p /workspace/formacao-engenharia-dados/desafio/raw
mkdir -p /workspace/formacao-engenharia-dados/desafio/gold

docker exec namenode hdfs dfs -mkdir -p /datalake/desafio/raw
docker exec namenode hdfs dfs -mkdir -p /datalake/desafio/gold

echo "Finalizado com sucesso"