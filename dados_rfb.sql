-- Criar a base de dados "dados_rfb"
CREATE DATABASE "dados_rfb"
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    CONNECTION LIMIT = -1;

COMMENT ON DATABASE "dados_rfb"
    IS 'Base de dados dos dados públicos de CNPJ da Receita Federal do Brasil';
