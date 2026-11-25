CREATE DATABASE shopbrasil_dw;

\c shopbrasil_dw;

CREATE TABLE IF NOT EXISTS metricas_categorias_snapshot (
    data_referencia DATE NOT NULL,
    categoria VARCHAR(100) NOT NULL,
    preco_medio NUMERIC(10, 2) NOT NULL,
    preco_minimo NUMERIC(10, 2) NOT NULL,
    preco_maximo NUMERIC(10, 2) NOT NULL,
    qtd_produtos INTEGER NOT NULL,
    atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_snapshot_data_categoria PRIMARY KEY (data_referencia, categoria)
);

CREATE TABLE IF NOT EXISTS metricas_categorias_historico (
    id SERIAL PRIMARY KEY,
    data_execucao TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    data_referencia DATE NOT NULL,
    categoria VARCHAR(100) NOT NULL,
    preco_medio NUMERIC(10, 2) NOT NULL,
    preco_minimo NUMERIC(10, 2) NOT NULL,
    preco_maximo NUMERIC(10, 2) NOT NULL,
    qtd_produtos INTEGER NOT NULL
);