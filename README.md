# Case Boticário - Data Engineer

## Parte 1: Arquitetura

Para a parte de arquitetura, é proposta a seguinte solução:

![Diagrama de Arquitetura](https://github.com/rmbborges/GB-DE-case/blob/main/assets/case_arquitetura.jpg?raw=true)

A Google Cloud foi escolhida por possuir maior familiaridade com as soluções e grande facilidade de integração entre estas. 

A solução foi desenvolvida tendo em vista dois grandes desafios: garantir que teremos poder de processamento o suficiente para atender grande volume de dados de uma empresa multinacional e garantir que diversas áreas da empresa possam acessar as informações com o devido controle de acesso.

**Eventos do SAP Hana / databases on-premise -> Kafka**: A transmissão de eventos pode ser realizada através do Apache Kafka. Essa é uma solução robusta com [conectores já desenvolvidos](https://github.com/SAP/kafka-connect-sap) que permite o streaming de eventos do sistema SAP, e também garante que os dados das estruturas que hoje estão on-premisse possam ser integradas de forma gradual.

**Kafka -> Cloud Storage**: Foi escolhido o Cloud Storage como destino dos dados em streaming por já existir uma solução de [conector](https://docs.confluent.io/kafka-connectors/gcs-sink/current/overview.html) e garantir que trabalharemos com alta performance e compatibilidade na transferência dos dados do Data Lake para o Banco Analítico. 

**Cloud Storage -> BigQuery**: Foi escolhido o BigQuery como banco de dados analítico. Essa é uma solução robusta que garante que consigamos trabalhar com um grande volume de dados.

**BigQuery -> BigQuery**: Ainda com o BigQuery, podemos construir camadas de dados que vão garantir consistência e usabilidade a informação sendo ingerida sem perder o dado histórico. Isso só é viável graças ao custo de armazenamento baixo desse produto.

**Processamento**: Para realizar o processamento dessas camadas de dados, existem algumas soluções. Uma delas é utilizar o Airflow (junto ao Cloud Composer) para orquestrar essas transformações de dados. Uma solução mais low-code, caso seja necessário, é utilizar o Data Fusion para criar a orquestração. Aqui, é importante ressaltar que múltiplas ferramentas de orquestração para as mesmas camadas analíticas, em geral, trazem dificuldade de governança para os pipelines de dados.

**Visualização**: Para visualização dos dados, as duas soluções propostas são Looker e DataStudio. O primeiro garante um maior controle de acesso e segurança da informação para as áreas de negócio e o segundo é uma opção menos custosa e fácil de ser desenvolver, porém perdemos a capacidade robusta do Looker de controlar acessos e edições de visualizações.

**Análises e Experimentação**: Por fim, temos algumas soluções para experimentação e análises de dados. O Vertex AI possui diversas soluções para que os times de Ciência de Dados e Machine Learning possam consumir os dados do ambiente analítico. Já Google Colab e Google Drive são soluções que podem ser úteis para garantir um acesso mais geral da empresa aos dados analíticos.

## Parte 2: Técnica

Para a solução da parte técnica, foi escolhido o Airflow como orquestrador. Além de ser uma solução open-source, ele é extremamente flexível e prático na criação de rotinas.

A nuvem escolhida foi a GCP pelo motivo de possuir maior familiaridade com os produtos. Foram utilizados o Google Cloud Storage para armazenamento de arquivos e o Google BigQuery para a criação das tabelas.

Como ponto de partida, foi assumido que os trẽs arquivos passados estavam inseridos em um bucket do Google Cloud Storage. 

### Antes de executar

Para execução desta solução é necessário, primeiro, fazer uma cópia deste repositório localmente e iniciar os containers. 

Como a imagem utilizada precisou ser modificada, foi criado um arquivo `Dockerfile` que parte da imagem oficial do Airflow 2.3.3. e instala duas dependências a mais de pacotes a serem utilizados.

```
docker-compose build
```

Após o build, deve-se executar a sequência de instalação do Airflow em seus containers:

```
docker-compose up airflow-init
```

Por fim, deve-se iniciar os containers:

```
docker-compose up
```

### Conexões
A seguinte conexão com a GCP precisa ser criada:
`gcp_boticario_de_case (google_cloud_platform)`

Na conta de serviço foram dadas as permissões de administração de dados do Google Cloud Storage e administração de dados do BigQuery.

### Variáveis 

Algumas variáveis precisam ser definidas:

```
# STORAGE
raw_sales_data_bucket: Bucket usado para ingerir os arquivos XLSX
sales_data_bucket: Bucket usado para armazenar os arquivos CSV (transformações dos arquivos XLSX)
twitter_data_bucket: Bucket usado para armazenar o resultado das requisições dos tweets

# BIGQUERY
bigquery_project_id: Projeto de armazenamento das tabelas do BigQuery
raw_sales_dataset: Nome do dataset para os dados crus de venda no BigQuery
raw_sales_table: Nome da tabela para os dados crus de venda no BigQuery
raw_twitter_dataset: Nome do dataset para os dados crus dos tweets no BigQuery
raw_tweets_table: Nome da tabela para os dados crus de venda no BigQuery
datawarehouse_sales_dataset: Nome do dataset usado para armazenar as tabelas agregadas dos dados de venda
datawarehouse_tweets_dataset: Nome do dataset usado para armazenar a tabela com os dados mais recentes dos tweets.

# TOKENS
twitter_api_bearer_token: Token usado para autenticação na API V2 do Twitter 

```

### DAGs

Foram criadas 4 dags: 

1. `sales/load_raw_sales_data_dag`: Esta DAG é responsável por fazer a carga dos arquivos passados em uma tabela crua do BigQuery. Para isso, primeiro foi necessário converter os arquivos do formato XLSX para CSV. Este tratamento é feito pelo operator personalizado `TreatXLSXOperator`, que converte os arquivos e faz a carga destes em um outro bucket. A partir daí, a DAG executa a criação de um Dataset vazio no BigQuery (caso não exista) e a criação da tabela crua `sales`. O modo de criação utilizado foi `WRITE_TRUNCATE` pois, por não possuir um identificador único de venda, o tratamento de duplicidades poderia trazer inconsistências.

2. `sales/create_sales_datawarehouse_tables_dag`: A segunda DAG é responsável por criar as tabelas modeladas de vendas no BigQuery. Para fazer isso, primeiro ela executa a criação de um Dataset vazio (caso não exista) e, após isso, a criação das tabelas modeladas através das queries criadas no módulo `helpers/SqlQueries`.
   
3. `twitter/request_and_load_raw_twitter_dag`: Esta DAG é responsável por executar a requisição dos tweets e carga em uma tabela crua. Para fazer isso, um hook e um operador personalizados foram criados. O `GetTopProductHook` é responsável por consultar na tabela `product_monthly_sales` o produto mais vendido para determinado mês e ano e retornar essa informação. Já o `RequestTwitterDataOperator` é responsável por realizar a requisição via API V2 do Twitter - os 50 tweets mais recentes citando "Boticário + Produto" - e salvar o resultado dessa requisição em um bucket do Google Cloud Storage definido. Por fim, esta dag cria um dataset vazio (caso não exista) e salva a tabela crua de tweets. Nesta dag, o modo de criação utilizado foi `WRITE_APPEND` pois temos o identificador do tweet, o que permite que trabalhemos com os dados mais recentes para um determinado ID.

4. `twitter/create_twitter_datawarehouse_dag`: A última DAG é responsável por criar uma tabela sem duplicatas para os tweets. Ela cria, inicialmente, um dataset vazio (caso não exista) e popula uma tabela com o dado mais recente dos tweets. 

Em uma primeira execução, é importante garantir que a ordem dessas DAGs seja respeitada para criação das dependências. Porém, após a primeira execução, estas podem ser executadas de forma independente.