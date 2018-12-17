# Data Scientist: Dito
Susumu Asaga

##  SQL

1. **Qual o nome, email e telefone das 5 pessoas que mais geraram receita?**

A consulta SQL que resolve a pergunta é a seguinte:
```SQL
WITH top5 AS (
  SELECT id, SUM(properties.revenue) revenue
  FROM `dito-data-scientist-challenge.tracking.dito`
  WHERE type = 'track'
  GROUP BY id
  ORDER BY revenue DESC
  LIMIT 5
)
SELECT id, name, email, phone, revenue FROM(
  SELECT identify.id,
    FIRST_VALUE(identify.traits.name IGNORE NULLS) OVER (
      PARTITION BY identify.id
      ORDER BY identify.timestamp DESC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) name,
    FIRST_VALUE(identify.traits.email IGNORE NULLS) OVER (
      PARTITION BY identify.id
      ORDER BY identify.timestamp DESC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) email,
    FIRST_VALUE(identify.traits.phone IGNORE NULLS) OVER (
      PARTITION BY identify.id
      ORDER BY identify.timestamp DESC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) phone,
    revenue
  FROM `dito-data-scientist-challenge.tracking.dito` identify, top5
  WHERE identify.type = 'identify' AND identify.id = top5.id
)
GROUP BY id, name, email, phone, revenue
ORDER BY revenue DESC
```
e a resposta da pergunta é a seguinte:

Row	| id | name	| email	| phone	| revenue
--- | --- | --- | --- | --- | ---
1 | f56861d3-a2b5-4ab0-b0ef-6ce2005f84dd | Heloísa Ordonhes | joshua_prosacco@gmail.com | (51) 91972-9639 | 4,850.27
2 | bea16033-7519-4520-a2ba-0425174d397b | Mirella Soares | velia.pagac@hotmail.com | (43) 97290-9288 | 4,433.44
3 | 9e505eb0-e3f2-4715-a1e9-e05605022a95 | Fernanda Resende | charlott.ledner@live.com | (33) 92311-0450 | 4,289.69
4 | 99ab1cee-2645-4ae3-8ffe-f28508666098 | Sophie Banheira | fransisca.predovic@bol.com.br | (24) 91404-3805 | 4,261.43
5 | fc3650a4-3f19-4047-9354-65e2a23a4a40 | Alícia Pinheira | bill@bol.com.br | (51) 91012-4779 | 4,260.19

A primeira sub-consulta `top5` é a consulta principal onde encontramos os `id`s das 5 pessoas que mais geraram receita.

Na sub-consulta mais interna é feito um `join` entre eventos de `identify` e a sub-consulta `top5` e usamos a função analítica `FIRST_VALUE` para obter a informação mais atualizada de `nome`, `email` e `phone` da pessoa.

Na sub-consulta mais externa agrupamos as linhas por pessoa e ordenamos por `revenue` descendente.

2. **De quantos em quantos dias, em média, as pessoas compram? Use a mediana como média.**

A consulta SQL que resolve a pergunta é a seguinte:
```SQL
SELECT PERCENTILE_CONT(
  TIMESTAMP_DIFF(timestamp, last_timestamp, SECOND)/86400,
  0.5
) OVER() median_days
FROM (
  SELECT id,
    timestamp,
    LAG(timestamp) OVER (
      PARTITION BY id
      ORDER BY timestamp
    ) last_timestamp
  FROM `dito-data-scientist-challenge.tracking.dito`
  WHERE type = 'track'
)
LIMIT 1
```
A resposta da consulta é a cada 0.221 dias.

A sub-consulta recupera para cada compra o `id` do comprador, o `timestamp`, e o `timestamp` da sua compra anterior ou `NULL` se for sua primeira compra, para isso usa a função analítica `LAG`.

A consulta principal calcula o intervalo em dias entre duas compras sucessivas usando a função `TIMESTAMP_DIFF` e a mediana de todas as linhas por meio da função analítica `PERCENTILE_CONT`.
