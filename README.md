# Pipeline de dados


## Objetivos
O objetivo deste projeto é desenvolver um pipeline para movimentar dados a partir de uma fonte inicial (flatfile) até a apresentação de dashboard para tomada de decisão sobre plataforma Power BI.

1. Arquivos disponibilizados em diretório _ad-hoc_
2. Transferência dos arquivos para pasta /raw (_servidor de borda_)
3. Transferência dos arquivos para filesystem HDFS, dentro do servidor _Hadoop_
4. Criação de estrutura de tabelas e carga na base relacional _Hive_
5. Desenvolvimento e transformação em tabelas dimensionais e persistência das mesmas
6. Desenvolvimento de dashboard (Power BI) e configuração de conexão com servidor remoto.