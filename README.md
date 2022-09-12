# Case Boticário - Data Engineer

## Parte 1: Arquitetura

## Parte 2: Técnica

Para a solução da parte técnica, foi escolhido o Airflow como orquestrador. Além de ser uma solução open-source, ele é extremamente flexível e prático na criação de rotinas.

A nuvem escolhida foi a GCP pelo motivo de possuir maior familiaridade com os produtos. Foram utilizados o Google Cloud Storage para armazenamento de arquivos e o Google BigQuery para a criação das tabelas.

Como ponto de partida, foi assumido que os trẽs arquivos passados estavam inseridos em um bucket do Google Cloud Storage. 

### Para executar

Para execução desta solução é necessário, primeiro, fazer uma cópia deste repositório localmente e iniciar os containers. 

Como a imagem utilizada precisou ser modificada, foi criado um arquivo `Dockerfile` que parte da imagem oficial do Airflow 2.3.3. e instala duas dependências a mais de pacotes a serem utilizados.

```docker-compose build```

Após o build, deve-se executar a sequência de instalação do Airflow em seus containers:

```docker-compose up airflow-init```

Por fim, deve-se iniciar os containers:

```docker-compose up```

### DAGs

Foram criadas 4 dags: 

1. `sales/load_raw_sales_data_dag`: Esta DAG é responsável por fazer a carga dos arquivos passados em uma tabela crua do BigQuery. Para isso, primeiro foi necessário converter os arquivos do formato XLSX para CSV. Este tratamento é feito pelo operator personalizado `TreatXLSXOperator`, que converte os arquivos e faz a carga destes em um outro bucket. A partir daí, a DAG executa a criação de um Dataset vazio no BigQuery (caso não exista) e a criação da tabela crua `sales`. O modo de criação utilizado foi `WRITE_TRUNCATE` pois, por não possuir um identificador único de venda, o tratamento de duplicidades poderia trazer inconsistências.

2. `sales/create_sales_datawarehouse_tables_dag`: A segunda DAG é responsável por criar as tabelas modeladas de vendas no BigQuery. Para fazer isso, primeiro ela executa a criação de um Dataset vazio (caso não exista) e, após isso, a criação das tabelas modeladas através das queries criadas no módulo `helpers/SqlQueries`.
   
3. `twitter/request_and_load_raw_twitter_dag`: Esta DAG é responsável por executar a requisição dos tweets e carga em uma tabela crua. Para fazer isso, um hook e um operador personalizados foram criados. O `GetTopProductHook` é responsável por consultar na tabela `product_monthly_sales` o produto mais vendido para determinado mês e ano e retornar essa informação. Já o `RequestTwitterDataOperator` é responsável por realizar a requisição via API V2 do Twitter - os 50 tweets mais recentes citando "Boticário + Produto" - e salvar o resultado dessa requisição em um bucket do Google Cloud Storage definido. Por fim, esta dag cria um dataset vazio (caso não exista) e salva a tabela crua de tweets. Nesta dag, o modo de criação utilizado foi `WRITE_APPEND` pois temos o identificador do tweet, o que permite que trabalhemos com os dados mais recentes para um determinado ID.

4. `twitter/create_twitter_datawarehouse_dag`: A última DAG é responsável por criar uma tabela sem duplicatas para os tweets. Ela cria, inicialmente, um dataset vazio (caso não exista) e popula uma tabela com o dado mais recente dos tweets. 

Em uma primeira execução, é importante garantir que a ordem dessas DAGs seja respeitada para criação das dependências. Porém, após a primeira execução, estas podem ser executadas de forma independente.