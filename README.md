A prática desta aula foi fascinante.

Como Executar o Projeto.

1. Pré-requisitos

Docker e Docker Compose instalados.

2. Preparar diretórios e permissões (Linux/macOS)

Na raiz do repositório, crie as pastas mapeadas pelo Compose e defina o AIRFLOW_UID no arquivo .env:

mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env


3. Inicializar o ambiente e subir os containers

Execute a inicialização do banco de metadados e construa a imagem customizada do Airflow:

docker compose up airflow-init
docker compose up -d --build


Nota: Caso o volume do PostgreSQL já tenha sido inicializado anteriormente sem o banco analítico, execute o script DDL manualmente uma única vez:

docker compose exec -T postgres psql -U airflow -d postgres < sql/init.sql


4. Configurar o Pool de Concorrência (ecommerce_pool)

Crie o pool com 2 slots exigido pelas tasks mapeadas dinamicamente:

docker compose exec airflow-apiserver airflow pools set ecommerce_pool 2 "Pool para processamento paralelo de categorias"


5. Acessar a Interface Web e Executar a DAG

Acesse http://localhost:8080 no navegador.

Faça login com as credenciais padrão:

Usuário: airflow

Senha: airflow

Localize a DAG shopbrasil_pricing_pipeline, ative-a (Unpause) e clique em Trigger DAG para disparar uma execução manual.

Validação dos Dados e Idempotência

Para verificar os resultados gravados no banco analítico shopbrasil_dw após a execução (e confirmar que múltiplas execuções no mesmo dia atualizam o snapshot sem duplicar linhas):

# Consultar tabela snapshot (1 linha por categoria na data de referência)
docker compose exec postgres psql -U airflow -d shopbrasil_dw -c "SELECT * FROM metricas_categorias_snapshot;"

# Consultar histórico acumulado (modo append por execução)
docker compose exec postgres psql -U airflow -d shopbrasil_dw -c "SELECT * FROM metricas_categorias_historico ORDER BY data_execucao DESC;"


Encerrando o ambiente

Para parar os containers e liberar recursos da máquina ao finalizar os testes:

docker compose down