-- Operacao do dia a dia de uma Streaming Table com SCHEDULE (SQL Editor)

-- atualizar agora, fora do agendamento (incremental)
REFRESH STREAMING TABLE fininsight_dev.bronze.bz_b3_cotacoes_sql;

-- reprocessar tudo do zero (cuidado: le todos os arquivos de novo)
REFRESH STREAMING TABLE fininsight_dev.bronze.bz_b3_cotacoes_sql FULL;

-- trocar o agendamento para todo dia
ALTER STREAMING TABLE fininsight_dev.bronze.bz_b3_cotacoes_sql
  ALTER SCHEDULE EVERY 1 DAY;

-- agendamento por CRON (Quartz), 07:00 de segunda a sexta, fuso de Sao Paulo
ALTER STREAMING TABLE fininsight_dev.bronze.bz_b3_cotacoes_sql
  ALTER SCHEDULE CRON '0 0 7 ? * MON-FRI' AT TIME ZONE 'America/Sao_Paulo';

-- remover o agendamento (a tabela continua existindo; so deixa de atualizar sozinha)
ALTER STREAMING TABLE fininsight_dev.bronze.bz_b3_cotacoes_sql DROP SCHEDULE;

-- ver o que foi criado: pipeline dedicado e agendamento
DESCRIBE EXTENDED fininsight_dev.bronze.bz_b3_cotacoes_sql;
