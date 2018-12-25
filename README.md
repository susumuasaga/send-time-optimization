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
e a resposta à pergunta é a seguinte:

Row	| id | name	| email	| phone	| revenue
--- | --- | --- | --- | --- | ---
1 | f56861d3-a2b5-4ab0-b0ef-6ce2005f84dd | Heloísa Ordonhes | joshua_prosacco@gmail.com | (51) 91972-9639 | 4,850.27
2 | bea16033-7519-4520-a2ba-0425174d397b | Mirella Soares | velia.pagac@hotmail.com | (43) 97290-9288 | 4,433.44
3 | 9e505eb0-e3f2-4715-a1e9-e05605022a95 | Fernanda Resende | charlott.ledner@live.com | (33) 92311-0450 | 4,289.69
4 | 99ab1cee-2645-4ae3-8ffe-f28508666098 | Sophie Banheira | fransisca.predovic@bol.com.br | (24) 91404-3805 | 4,261.43
5 | fc3650a4-3f19-4047-9354-65e2a23a4a40 | Alícia Pinheira | bill@bol.com.br | (51) 91012-4779 | 4,260.19

A primeira sub-consulta `top5` é a consulta principal onde encontramos os `id`s das 5 pessoas que mais geraram receita.

Na sub-consulta mais interna é feito um `join` entre eventos de `identify` e a sub-consulta `top5` e usamos a função analítica `FIRST_VALUE` para obter a informação mais atualizada de `name`, `email` e `phone` da pessoa.

Na sub-consulta mais externa agrupamos as linhas por pessoa e ordenamos por `revenue` descendente.

2. **De quantos em quantos dias, em média, as pessoas compram? Use a mediana como média.**

A consulta SQL que resolve a pergunta é a seguinte:
```SQL
SELECT PERCENTILE_CONT(
  TIMESTAMP_DIFF(timestamp, last_timestamp, SECOND) / 86400,
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
A resposta da consulta é que as pessoas compram em média a cada 0.221 dias.

A sub-consulta recupera para cada compra o `id` do comprador, o `timestamp`, e o `timestamp` da sua compra anterior ou `NULL` se for sua primeira compra, para isso usa a função analítica `LAG`.

A consulta principal calcula o intervalo em dias entre duas compras sucessivas usando a função `TIMESTAMP_DIFF` e a mediana de todas as linhas por meio da função analítica `PERCENTILE_CONT`.

## Send Time Optimization

Todos são diferentes e todos têm horários diferentes, mas podem se agrupar ao redor de poucas horas específicas diferentes do dia, especialmente se estăo no mesmo fuso horário, pois a maioria trabalha de dia e dorme de noite. Então a primeira coisa que vamos fazer é processar os dados e descobrir quais são esses grupos de horas e depois encontrar qual é o grupo preferencial de cada usuário para enviar e-mail, respeitando o direito daqueles que não desejam receber as nossas mensagens. Para isso a técnica de ciência de dados que usaremos é a aprendizagem não supervisionada de análise de cluster.

Além do ciclo diário existe também o ciclo semanal, assim os hábitos dos usuários no fim de semana devem ser diferentes dos hábitos no meio da semana ou da segunda-feira, assim  devemos considerar não só a hora onde os eventos de abertura ocorrem, mas também em que dia da semana aconteceu.

### Análise de cluster

Para fazer a análise de cluster, consideraremos apenas a hora e o dia da semana dos eventos `open` e  abstraíremos os demais eventos, bem como o `id`, `email_id`, o dia, mês, e ano. Quando dois eventos `open` ocorrerem com intervalo inferior a 1 segundo, consideramos como se tivesse ocorrido apenas um.

Assim temos apenas 1 dimensăo a considerar: quando o `open` ocorreu durante a semana. Podemos analisar os dados usando a técnica de Kernel Density Estimation (KDE).

As estimativas de densidade estão intimamente relacionadas com histogramas, mas são dotadas com propriedades como suavidade e continuidade usando um kernel e largura de banda adequadas. Como kernel usamos a função de Epanechnikov e para largura da banda adotamos o valor de 1 hora, que foi escolhida varrendo-se uma série de 25 valores por década e escolhendo o menor valor possível que visualmente fosse suave, sem exagerar e perder características marcantes da distribuição.

Esperamos um aumento no open rate enviando a mensagem um ceto tempo antes, digamos 30 minutos, de um máximo local da densidade estimada pelo KDE.

Para facilitar a análise elaboraremos o KDE para cada dia da semana separadamente. Para assegurar a acurácia no início e fim do dia, incluímos os dados de 1:15 antes da zero hora, no dia anterior, e de 1:15 depois da meia-noite, no dia seguinte.
