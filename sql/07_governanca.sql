-- SQL Editor, logado como admin do catalogo. Executar uma vez (ou versionar este script).

-- 1) Permissoes minimas para um grupo de analistas ler a Gold
GRANT USE CATALOG ON CATALOG fininsight_dev TO `analistas`;
GRANT USE SCHEMA, SELECT ON SCHEMA fininsight_dev.gold TO `analistas`;

-- 2) Mascaramento de coluna (LGPD): tabela de demonstracao com documento do cliente
CREATE TABLE IF NOT EXISTS fininsight_dev.gold.dim_cliente_demo (
  cliente_id INT, nome STRING, documento STRING
);
INSERT INTO fininsight_dev.gold.dim_cliente_demo VALUES
  (1, 'Maria Souza', '123.456.789-09'), (2, 'Joao Lima', '987.654.321-00');

CREATE OR REPLACE FUNCTION fininsight_dev.gold.mascara_documento(doc STRING)
RETURNS STRING
RETURN CASE WHEN is_account_group_member('admins') THEN doc
            ELSE concat('***.***.***-', right(doc, 2)) END;

ALTER TABLE fininsight_dev.gold.dim_cliente_demo
  ALTER COLUMN documento SET MASK fininsight_dev.gold.mascara_documento;

-- 3) Filtro de linhas: analistas veem apenas ativos brasileiros
CREATE OR REPLACE FUNCTION fininsight_dev.gold.filtro_pais(pais STRING)
RETURNS BOOLEAN
RETURN is_account_group_member('admins') OR pais = 'BR';

ALTER TABLE fininsight_dev.gold.dim_ativo SET ROW FILTER fininsight_dev.gold.filtro_pais ON (pais);

-- 4) Conferir quem pode o que
SHOW GRANTS ON SCHEMA fininsight_dev.gold;
