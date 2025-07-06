# ETL - Extract, Transform, Load

## baseado no modelo de:
 - https://github.com/aphonsoar/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ

## Arquivos de CNPJ dados Abertos RFB
Objetivo: Baixar os arquivos dados abertos da RFB dos CNPJs e inserir no banco de dados, para consultas locais.

* Este ETL é executado em segundo plano e verifica qual a data mais atual e baixa os arquivos, extrai, dopra a tabela e salva no banco de dados, exlcui os arquivos baixados e extraidos e gera os índices das tabelas.
banco de dados: Postgres

* tempo médio de execução 8 horas

## primeiro gere o banco de dados
utilize o arquivo dados_rfb para gerar o banco de dados.

## Configure o arquivo .env para conexão com o banco de dados
```
DB_HOST=
DB_PORT=
DB_USER=
DB_PASSWORD=
DB_NAME=
```

## Entre na pasta do projeto crie o Ambiente Virtual e instale os requirements.txt e depois saia do ambiente virtual.
```
cd /opt/dados_rfb
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
deactivate
```

### No Windows para ativar o ambiente virtual, entra na pasta do seu projeto e use:
```
.\venv\Scripts\activate
python .\etl_rfb_dados.py --etl
```

### No Linux execute o para rodar automaticamente .py para rodar em segundo plano:
```
sudo ./run_etl.sh
```

Veja os processos em execução:
```
ps aux | grep etl_rfb_dados.py
```

Para acompanhar em tempo real:
```
tail -f /opt/dados_rfb/logs/etl_rfb_dados_log.txt
```

Para para a execução:
```
kill -9 <PID>
```
