# PIPELINE DE DADOS DE VENDAS

![Shell Script](https://img.shields.io/badge/shell_script-%23121011.svg?style=for-the-badge&logo=gnu-bash&logoColor=white) ![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54) ![Apache Hadoop](https://img.shields.io/badge/Apache%20Hadoop-66CCFF?style=for-the-badge&logo=apachehadoop&logoColor=black) ![Apache Hive](https://img.shields.io/badge/Apache%20Hive-FDEE21?style=for-the-badge&logo=apachehive&logoColor=black) 

## Objetivo
O objetivo deste projeto é desenvolver um pipeline para movimentar dados a partir de uma fonte inicial (_flatfile_) até a apresentação de dashboard para tomada de decisão sobre plataforma Power BI.

### Resumo do fluxo
1. Arquivos disponibilizados em diretório _ad-hoc_ ``/raw``. Esta pasta indica uma local de origem externo, que poderia ser também um recurso online, API, etc.
2. Criação de ambiente: utilizando o script ``01_criacao_ambiente.sh`` é possível configurar o ambiente, criando os diretórios necessários à operação. O nomes dos mesmos e ``paths`` devem ser configurados no arquivo ``config.ini``   
3. Transferência dos arquivos para  o _servidor de borda_, ``/desafio/raw``. Utilizando o script disponível em `` 02_source_to_edge.sh``
4. Transferência dos arquivos para _filesystem_ HDFS, dentro do servidor _Hadoop_ [HDFS /datalake/desafio/raw/clientes] [04_edge_to_hdfs.sh]
5. Criação de estrutura de tabelas e carga na base relacional _Hive_ [07_process]
6. Desenvolvimento e transformação em tabelas dimensionais e persistência das mesmas [pendente]
7. Finalmente, desenvolvimento de dashboard (Power BI) em ``/desafio/app/Projeto Vendas.pbix``, esta implementação utiliza as tabelas dimensionais disponibilizadas em ``/desafio/gold`` 

## Github, gitpod

- Ambiente em nuvem


## Estrutura de arquivos
```
root
|
+ desafio
+- raw
    +- vendas.csv
    +- hello
+- gold
+- app
+- run
+ raw
```

# Trilha para se tornar um desenvolvedor big data



### O que é big data

grandes conjuntos de dados que precisam ser processados e armazenados: 

3Vs Velocidade, Volume, Variedade

modelo relacional x modelo dimensional

## O ambiente

docker, docker-compose

python 
pyspark 
HDFS
namenode
hive-server
jupyter and pyspark

## Paso a passo

### 1. Criação do ambiente
Processo inicia com um conjunto de arquivos na pasta ```/raw```, estes arquivo possuem dados de vendas.

|Tabela|Formato|Tamanho|Detalhes|
|------|-------|-------|--------|
|VENDAS|csv|x.xx Mb|Informações de vendas, preços, descontos, etc
|CLIENTES|csv|x.xx Mb|Informações dos clientes|
|ENDERECO|csv|x.xx Mb|Informações do endereço (ligada aos clientes)
|REGIAO|csv|x.xx Mb|Região 
|??|csv|x.xx Mb|??

### 2. Movimentar os arquivos fonte
O primeiro passo deste processo será movimentar os arquivos da fonte, que poderiam estar em outro servidor ou formato, para a primeira pasta do nosso servidor. Chamaremos esta pasta de servidor de borda, já que entendemos que será o local de entrada das nossas informações. 
    1. Arquivo ```/desafio/scripts/config.ini```: utilizamos um arquivo centralizado contendo as variáveis que serão utilizadas no projeto. Decidimos pela utilização deste formato já que atende tanto os scripts do tipo ```bash```, quanto ```python``` 
    2. Arquivo ```/desafio/scripts/move_files_to_edge.sh```: este primeiro script recupera o nome das entidades (arquivos) que se encontram na pasta de origem e itera o nome de cada um deles realizando 2 ações principais: 
       1. Mover o arquivo para a pasta informada
       2. Alterar o nome dos arquivos para lowercase
    3. Para rodar o script deve executar: 
       1. ```$ cd /desafio/scripts```
       2. ```$ bash move_files_to_edge.sh```
    4. O resultado desta operação deve finalizar na cópia dos arquivos no diretório ```/desafio/raw``` 

### 3. Movimentação de arquivos para HDFS
O segundo passo será movimentar os arquivos recebidos para o servidor HDFS que está conteinerizado.
   1. Para realizar esta operação será necessário executar o script ```/desafio/scripts/move_to_hdfs.sh``` que fará a operação:
   2. Os arquivos serão movimentados para a pasta HDFS ```/datalake/desafio/raw``` 
   
### 4. Criação dinámica de DLLs
Uma vez que as informações estejam disponíveis no servidor HDFS, será o momento de criar os DLLs para as tabelas. 
   1. Através de um script ```python```, será lido cada um dos arquivos e extraído deles os cabeçalhos com o nome das colunas. 
   2. Com esta informação, será criado - para cada tabela - script de criação ```hql``` que posteriormente será chamado através de um comando *beeline* ```hive```. 
   3. Estes scripts podem ser encontrados na pasta ```/desafio/scripts/hql/create_table_[nome_da_tabela].hql``` 

### 5. Criação de tabelas
Com as informações no servidor Hadoop e as DLLs criadas, chega o momento de executar a criação das bases de dados, tabelas e carga de dados. 
   1. Para realizar esta operação, o script ```/desafio/scripts/upload_to_hive.sh``` deve, mas uma vez, recuperar os nomes de todas as tabelas e processar para cada uma delas, a criação das tabelas externas e gerenciadas, assim como realizar a vinculação das mesmas a partir dos arquivos já mencionados.
   2. O resultado de estas operações deve apresentar a seguinte estrutura no Hive:
      1. 2 bancos de dados: ```desafio_db_ext``` e ```desafio_db_stg```; 
      2. Para cada um destes bancos teremos as tabelas ```vandas, clientes, endereco, regiao, ???```

![diagrama_relacional](./images/diagrama_relacional.png)

### 6. Tratamento dos dados
Com todos os dados consolidados na base, devemos trabalhar os dados, seguindo os critérios: a) Strings vazias ou nulas = "Não informado", b) Números nulos = 0.0
   1. Para realizar esta operação devemos rodar script ```python``` no ambiente pyspark. Realizaremos as seguintes operações:
      1. Tratamento das strings vazias [explicar]
      2. Tratamento dos números [explicar]
      3. Tratamento das datas [explicar]

como rodar?

```docker exec jupyter-spark /opt/spark-2.4.1-bin-without-hadoop/bin/spark-submit /desafio/scripts/process/process.py``` 

### 7. Tabelas dimensionais 
Com todas as informações consistentes, partimos para a organização da informação em tabelas dimensionais, que diferente das tabelas relacionais do Hive, estão desnormalizadas e devem ser montadas [como mostrado na figura]. 

|Tabela|Obs|
|-|-|
|FT_VENDAS|Tabela fato com os dados de vendas|
|DIM_CLIENTES|Dimensão de clientes com a informação do nome|
|DIM_LOCALIDADE|Dimensão com informações geográficas como Estado, cidade ou país|
|DIM_TEMPO|A dimensão de tempo, a través da data de fatura, foi extraído ano, mês e trimestre. |

![modelo_dimensional](images/diagrama_dimensional.png)



## Rascunhos 

> explicação de andre para config do acesso remoto: 3:16 do dia 20/6
> 
``ssh -L 10000:pveinbergin-formacaoeng-pqpkh8mabez:10000 pveinbergin-formacaoeng-pqpkh8mabez#HaaV_aeKfaSdVlMSluw3zPLYLIDKG1kC@pveinbergin-formacaoeng-pqpkh8mabez.ssh.ws-us101.gitpod.io`` 

``ssh pveinbergin-formacaoeng-pqpkh8mabez#HaaV_aeKfaSdVlMSluw3zPLYLIDKG1kC@pveinbergin-formacaoeng-pqpkh8mabez.ssh.ws-us101.gitpod.io/desafio/gold``

``ssh pveinbergin-formacaoeng-pqpkh8mabez#AVGrGF1mJ6IXsbdLWBXTh7t.TsZjm43v@pveinbergin-formacaoeng-pqpkh8mabez.ssh.ws-us101.gitpod.io/desafio``