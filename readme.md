# ETL - Extract, Transform, Load

## baseado no modelo de:

- https://github.com/aphonsoar/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ

# Dados Públicos CNPJ

- Fonte oficial da Receita Federal do Brasil, [aqui](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj).
- Layout dos arquivos, [aqui](https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf).

A Receita Federal do Brasil disponibiliza bases com os dados públicos do cadastro nacional de pessoas jurídicas (CNPJ).

De forma geral, nelas constam as mesmas informações que conseguimos ver no cartão do CNPJ, quando fazemos uma consulta individual, acrescidas de outros dados de Simples Nacional, sócios e etc. Análises muito ricas podem sair desses dados, desde econômicas, mercadológicas até investigações.

Nesse repositório consta um processo de ETL para:
    **i)** verifica se os dados disponibilizados estãoa atualizados no banco atual, se não estiverem;
    **i)** baixa os arquivos;
    **ii)** descompactar;
    **iii)** ler, tratar;
    **iv)** apaga as tabelas existentes e cria novas para inserir os dados no banco de dados;
    **v)** cria os indices no PostgreSQL, e;
    **vi)** exclui os arquivos baixados e descompactados.

observações:

* os dados abertos da RFB são completos, não são incrementais, por isso sempre que rodar o ETL ele irá apagar a tabela e criar uma nova do zero para incluir todos os dados novamente.
* tempo médio de execução 8 horas (utilizando um i3 2328m, 16GB ram)

---

# Infraestrutura mínima necessária:

- **Necessário pelo menos 30GB de espaço livre em disco** para armazenar os arquivos e os arquivos (originas e descompactados), após o processo ele irão ser excluídos.
- [Python 3.8](https://www.python.org/downloads/release/python-3810/)
- [PostgreSQL 14.2](https://www.postgresql.org/download/)

---

# Pastas e arquivos gerados:

- `logs`: pasta que registra os logs do processamento em tempo real do arquivo `etl_rfb_dados.py` com o nome `etl_rfb_dados_log.txt`
- `files_downloaded`: pasta onde os arquivos que serão baixados do site da RFB. obs: esses arquivos serão excluidos após a inserção no banco
- `files_extracted`: pasta onde os arquivos extraídos ficam depois de baixados. obs: esses arquivos serão excluidos após a inserção no banco
- `files_error`: pasta com os arquivos que deram erro no processamento. é gerado um log somente de erros `arquivos_com_erro.txt` na pasta dados_rfb

# Tabelas geradas:

> `empresa`: dados cadastrais da empresa em nível de matriz
>
> `empresa_porte`: tabela auxiliar com dados com o porte da empresa
>
> `estabelecimento`: dados analíticos da empresa por unidade / estabelecimento (telefones, endereço, filial, etc)
>
> `estabelecimento_situacao_cadastral`: tabela auxiliar com dados da situacao cadastral das empresas
>
> `info_dados`: dados da ultima atualização dos dados do ETL para verificar se os dados estão atualizados de acordo com o site da RFB
>
> `socios`: dados cadastrais dos sócios das empresas
>
> `socios_identificador`: tabela auxiliar com dados do tipo de sócio
>
> `simples`: dados de MEI e Simples Nacional
>
> `cnae`: código e descrição dos CNAEs
>
> `quals`: tabela de qualificação das pessoas físicas - sócios, responsável e representante legal.
>
> `natju`: tabela de naturezas jurídicas - código e descrição.
>
> `moti`: tabela de motivos da situação cadastral - código e descrição.
>
> `pais`: tabela de países - código e descrição.
>
> `munic`: tabela de municípios - código e descrição.

* No final do processamento é criado os índices nas tabelas

  - Pelo volume de dados, as tabelas  `empresa`, `estabelecimento`, `socios` e `simples` possuem índices para a coluna `cnpj_basico`, que é a principal chave de ligação entre elas.

Para maiores informações, consulte o:

- [Layout dos dados abertos do CNPJ](https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/arquivos/NOVOLAYOUTDOSDADOSABERTOSDOCNPJ.pdf).
- [Modelo de Entidade Relacionamento](https://github.com/aphonsoar/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ/blob/master/Dados_RFB_ERD.png)

---

# Como usar:

1. Com o Postgres instalado, inicie a instância do servidor (pode ser local) e crie o banco de dados conforme o arquivo `dados_rfb.sql` ou conforme abaixo.

- Conectar como postgres e executar o arquivo:
  sudo -u postgres psql -f dados_rfb.sql

  ```
   -- Criar a base de dados "dados_rfb"
  CREATEDATABASE "dados_rfb"
      WITH  
  OWNER= postgres
      ENCODING='UTF8'
      CONNECTIONLIMIT= -1;
  COMMENT ONDATABASE"dados_rfb"
      IS 'Base de dados dos dados públicos de CNPJ da Receita Federal do Brasil';
  ```

2. Crie um arquivo `.env` no diretório raiz, conforme as variáveis de ambiente do seu ambiente de trabalho (localhost) ou utilize como referência o arquivo `.env_template` você pode também, por exemplo, renomear o arquivo de `.env_template` para apenas `.env` e então utilizá-lo:

   - `DB_HOST`: host da conexão com o BD
   - `DB_PORT`: porta da conexão com o BD
   - `DB_USER`: usuário do banco de dados criado pelo arquivo `dados_rfb.sql`
   - `DB_PASSWORD`: senha do usuário do BD
   - `DB_NAME`: nome da base de dados na instância (`dados_rfb` - conforme arquivo `dados_rfb.sql`)
3. Instale as bibliotecas necessárias, disponíveis em `requirements.txt` dentro do Ambiente Virtual na pasta do projeto `dados_rfb`:

## No Linux:

- criando e ativando o ambiente virtual e instalando o requirements.txt, dentro pasta do seu projeto `dados_rfb`:

```
cd /opt/dados_rfb
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
deactivate
```

## No Windows:

- criando e ativando o ambiente virtual e instalando o requirements.txt, dentro pasta do seu projeto `dados_rfb`:

```
python3 -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
deactivate
```

---

## Execução do ETL:

### No linux:

``python3 etl_rfb_dados.py --etl``

- # de permissão de execução

  ``sudo chmod +x run_etl.sh``
- para executar em segundo plano:
  ``sudo ./run_etl.sh``
- Veja os processos em execução:
  ``ps aux | grep etl_rfb_dados.py``
- Para acompanhar em tempo real:
  ``tail -f /opt/dados_rfb/logs/etl_rfb_dados_log.txt``
- Para para a execução:
  ``kill -9 <PID>``

### No Windows

* execute Ambiente virtual `.\venv\Scripts\activate` depois o arquivo `python etl_rfb_dados.py --etl` e aguarde a finalização do processo.

- para acompanhar a execução em tempo real veja o arquivo em `dados_rfb\logs\etl_rfb_dados_log.txt`

# Bonus Docker!

# 1. Construa a imagem

docker compose build --no-cache

# 4. Suba o container

docker compose up -d

# 5. Verifique se está rodando - Deve mostrar status "Up"

docker ps

# 6. Agora execute o ETL

# Executar o script ETL (tem que esperar com o terminal aberto...)

docker exec -it dados_rfb-etl python etl_rfb_dados.py --etl

# Ou em segundo plano

docker exec -d dados_rfb-etl python etl_rfb_dados.py --etl

# ver Logs

docker exec dados_rfb-etl cat /code/logs/etl_rfb_dados_log.txt

# Acessar o container para debug

docker exec -it dados_rfb-etl bash

# Ver logs do container
docker logs dados_rfb-etl

# Seguir logs em tempo real (como tail -f)
docker logs -f dados_rfb-etl

# Ver últimas linhas
docker logs --tail 50 dados_rfb-etl

# Ver logs com timestamp
docker logs -t dados_rfb-etl

# Combinar opções
docker logs -f -t --tail 100 dados_rfb-etl

# Limpar a imagem antiga (opcional)
docker rmi dados_rfb-etl

# Construir com a nova sintaxe
docker build --no-cache -t dados_rfb-etl .

# !Parar o container

docker compose down
