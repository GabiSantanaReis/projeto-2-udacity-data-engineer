
# Sparkify Data Warehouse Project

## Introdução
Este projeto é para a startup de streaming de música Sparkify, que quer mover seus processos e dados para a nuvem. Usaremos um pipeline ETL para extrair dados de logs de atividade de usuários e metadados de músicas armazenados no S3, carregá-los em tabelas no Amazon Redshift, e transformá-los em um esquema dimensional para análise.

## Esquema do Banco de Dados
O esquema do banco de dados segue um formato estrela, com a tabela de fatos `songplays` e tabelas de dimensões `users`, `songs`, `artists` e `time`. Esse design permite consultas analíticas eficientes sobre os dados.

### Tabelas
- **songplays**: Registros de reprodução de músicas.
- **users**: Informações dos usuários.
- **songs**: Detalhes das músicas.
- **artists**: Detalhes dos artistas.
- **time**: Detalhes temporais das execuções.

## Pipeline ETL
1. **Extração**: Dados são extraídos de arquivos JSON no S3.
2. **Transformação**: Dados são transformados e organizados em tabelas de fatos e dimensões.
3. **Carga**: Dados transformados são carregados nas tabelas no Redshift.

## Consultas de Exemplo
- Música mais tocada:
  ```sql
  SELECT song_id, COUNT(*) AS play_count
  FROM songplays
  GROUP BY song_id
  ORDER BY play_count DESC
  LIMIT 1;
  ```
- Horário de maior uso por hora:
  ```sql
  SELECT EXTRACT(hour FROM start_time) AS hour, COUNT(*) AS play_count
  FROM songplays
  GROUP BY hour
  ORDER BY play_count DESC;
  ```

## Execução
1. Atualize as configurações em `dwh.cfg`.
2. Execute `create_tables.py` para criar as tabelas.
3. Execute `etl.py` para carregar os dados.

**Nota**: Lembre-se de excluir o cluster Redshift após concluir o projeto para evitar custos adicionais.
