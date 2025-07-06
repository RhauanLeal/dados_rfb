-- Criar a base de dados "dados_rfb"
-- caso nencessário altere o Owner
CREATE DATABASE "dados_rfb"
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    CONNECTION LIMIT = -1;

COMMENT ON DATABASE "dados_rfb"
    IS 'Base de dados para gravar os dados públicos de CNPJ da Receita Federal do Brasil';
