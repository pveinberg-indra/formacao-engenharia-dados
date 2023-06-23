# Pipeline de dados


## Objetivos
O objetivo deste projeto é desenvolver um pipeline para movimentar dados a partir de uma fonte inicial (flatfile) até a apresentação de dashboard para tomada de decisão sobre plataforma Power BI.

1. Arquivos disponibilizados em diretório _ad-hoc_ [/raw]
2. Transferência dos arquivos para pasta /raw (_servidor de borda_)[/desafio/raw] [02_source_to_edge.sh]
3. Transferência dos arquivos para filesystem HDFS, dentro do servidor _Hadoop_ [HDFS /datalake/desafio/raw/clientes] [04_edge_to_hdfs.sh]
4. Criação de estrutura de tabelas e carga na base relacional _Hive_ [07_process]
5. Desenvolvimento e transformação em tabelas dimensionais e persistência das mesmas [pendente]
6. Desenvolvimento de dashboard (Power BI) e configuração de conexão com servidor remoto.

## Estrutura de arquivos
```
root
|
+ desafio
|
+ raw
```