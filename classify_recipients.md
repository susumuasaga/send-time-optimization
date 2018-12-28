
# Classificação dos recebedores

Iniciamos com as importações padrões.


```python
import numpy as np
import pandas as pd
```

A seguir prosseguimos com a leitura dos seguintes conjuntos de dados:
* `events` é o conjunto de todos os eventos da base de dados com eliminação das linhas duplicadas;
* `recipients` é o conjunto de todos os recebedores. Incializamos os campos `send day` com 0 e `send time` com o `string` vazio. Estes campos serão preenchidos com o dia da semana e a hora de envio para otimizar a taxa de abertura para o recebedor.
* `clusters` é o conjunto de 13 clusters encontrados por meio do algoritmo de análise de clusters.


```python
events = pd.read_csv('events.csv')\
    .drop_duplicates(['id', 'timestamp', 'email_id', 'action'])
recipients = pd.read_csv('recipients.csv').set_index('id')
recipients['send day'] = 0
recipients['send time'] = ''
clusters = pd.read_csv('clusters.csv')
```

Para cada recebedor:
1. Se houver `action` igual a `unsubscribe` ou `spamreport` atribuir o valor -1 para `send day`, indicando para não enviar mais e-mails para o recebedor;
2. Senão verificar qual é o cluster quando ocorre a maior parte dos `open`s do recebedor;

  1. Atribuir o valor do dia da semana e a hora de envio encontrado para os campos `send day` e `send time`. Em caso de empate entre os clusters, escolher o cluster de rank mais alto. O valor de `send day` é um inteiro entre 1 e 7, sendo que 1 corresponde ao domingo e 7 ao sábado.

  2. Se não houver `open` em nenhum dos clusters deixar `send day` com o valor 0, indicando que ainda não foi possível encontrar a melhor hora para o envio para o recebedor e que há necessidade de levantar mais dados para determiná-la.

Por ser um processo demorado, a cada 1% processado o programa mostra uma mensagem na tela.


```python
percentis = np.linspace(0, len(recipients), 101);
percent = 0;
curr = 0;
for id, recipient in recipients.iterrows():
    if curr > percentis[percent + 1]:
        percent += 1
        print('{}% complete'.format(percent))
    curr += 1
    unsubscriptions = events.query(
        'id == @id and (action == "unsubscribe" or action == "spamreport")')
    if unsubscriptions.size:
        recipients.loc[id, 'send day'] = -1
    else:
        clusters['count'] = 0
        opens = events.query('id == @id and action == "open"')
        classified = False
        for index2, event in opens.iterrows():
            for index1, cluster in clusters.iterrows():
                if cluster['start'] <= event['daytime'] <= cluster['end']:
                    clusters.loc[index1, 'count'] += 1
                    classified = True
        if classified:
            index_max = clusters['count'].idxmax()
            recipients.loc[id, 'send day'] = clusters.loc[index_max, 'send day']
            recipients.loc[id, 'send time'] = clusters.loc[index_max, 'send time']

```

    1% complete
    2% complete
    3% complete
    4% complete
    5% complete
    6% complete
    7% complete
    8% complete
    9% complete
    10% complete
    11% complete
    12% complete
    13% complete
    14% complete
    15% complete
    16% complete
    17% complete
    18% complete
    19% complete
    20% complete
    21% complete
    22% complete
    23% complete
    24% complete
    25% complete
    26% complete
    27% complete
    28% complete
    29% complete
    30% complete
    31% complete
    32% complete
    33% complete
    34% complete
    35% complete
    36% complete
    37% complete
    38% complete
    39% complete
    40% complete
    41% complete
    42% complete
    43% complete
    44% complete
    45% complete
    46% complete
    47% complete
    48% complete
    49% complete
    50% complete
    51% complete
    52% complete
    53% complete
    54% complete
    55% complete
    56% complete
    57% complete
    58% complete
    59% complete
    60% complete
    61% complete
    62% complete
    63% complete
    64% complete
    65% complete
    66% complete
    67% complete
    68% complete
    69% complete
    70% complete
    71% complete
    72% complete
    73% complete
    74% complete
    75% complete
    76% complete
    77% complete
    78% complete
    79% complete
    80% complete
    81% complete
    82% complete
    83% complete
    84% complete
    85% complete
    86% complete
    87% complete
    88% complete
    89% complete
    90% complete
    91% complete
    92% complete
    93% complete
    94% complete
    95% complete
    96% complete
    97% complete
    98% complete
    99% complete


Vamos gravar e dar uma vista em alguns resultados.


```python
recipients.to_csv('recipients.csv')
recipients
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>count</th>
      <th>send day</th>
      <th>send time</th>
    </tr>
    <tr>
      <th>id</th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>fc7edfc56268400016851d22245ff57c66956b25</th>
      <td>930</td>
      <td>3</td>
      <td>17:30</td>
    </tr>
    <tr>
      <th>c5b8c2183afec62b5384ea6c02f14f769bc4a61f</th>
      <td>365</td>
      <td>3</td>
      <td>12:00</td>
    </tr>
    <tr>
      <th>2cd0335e35aa7b216a8cb15aaf5d844041d5c3d2</th>
      <td>264</td>
      <td>5</td>
      <td>10:30</td>
    </tr>
    <tr>
      <th>7a1ffa6b95b204188416133f2987eb1525f62211</th>
      <td>181</td>
      <td>3</td>
      <td>12:00</td>
    </tr>
    <tr>
      <th>d6fd732b813bbadc8c1c21e7e2374c6d3cdc52df</th>
      <td>177</td>
      <td>3</td>
      <td>17:30</td>
    </tr>
    <tr>
      <th>3157698d47d1b17609465f853f70e29cf5ec4795</th>
      <td>176</td>
      <td>3</td>
      <td>17:30</td>
    </tr>
    <tr>
      <th>e42bee202af8ef72213e3b3087f482e1ab685bfc</th>
      <td>176</td>
      <td>4</td>
      <td>18:15</td>
    </tr>
    <tr>
      <th>62a31a7468e3079e331cbb8886cd630648d888bf</th>
      <td>154</td>
      <td>3</td>
      <td>10:00</td>
    </tr>
    <tr>
      <th>addfc47ac6591044f01a02384aeebf4c00677729</th>
      <td>147</td>
      <td>5</td>
      <td>10:30</td>
    </tr>
    <tr>
      <th>f25241535605899bfe986d7c44f287e3391811c4</th>
      <td>147</td>
      <td>4</td>
      <td>18:15</td>
    </tr>
    <tr>
      <th>ff33262c93bfd0efe8fb621fe2483c6b72bb5491</th>
      <td>134</td>
      <td>5</td>
      <td>18:45</td>
    </tr>
    <tr>
      <th>be699290ab5f82fbd8e6b40d08a5148990550893</th>
      <td>132</td>
      <td>3</td>
      <td>12:00</td>
    </tr>
    <tr>
      <th>48ae6f94bcc45ca12e919c1a6b46b3dc62ac221d</th>
      <td>131</td>
      <td>5</td>
      <td>13:15</td>
    </tr>
    <tr>
      <th>29db5e5a0e6856a9cf15b5c06f73612f241f9867</th>
      <td>128</td>
      <td>3</td>
      <td>21:30</td>
    </tr>
    <tr>
      <th>670db61623efec8d08a71b3942f4c27674599d63</th>
      <td>124</td>
      <td>5</td>
      <td>10:30</td>
    </tr>
    <tr>
      <th>50214ff833f59484739164bba5be061c2e399281</th>
      <td>123</td>
      <td>5</td>
      <td>13:15</td>
    </tr>
    <tr>
      <th>f3508e83bf6687309062adccdb0ac8f81d92e0e3</th>
      <td>119</td>
      <td>5</td>
      <td>13:15</td>
    </tr>
    <tr>
      <th>8018f5e77841f83c2bae832431e5651056e64c2d</th>
      <td>117</td>
      <td>4</td>
      <td>14:00</td>
    </tr>
    <tr>
      <th>c58c7d99a03e97433b728c5618d071d03f4e65e3</th>
      <td>116</td>
      <td>2</td>
      <td>18:30</td>
    </tr>
    <tr>
      <th>fb856b4f65073b4ed842922e1fa0fa4de34f9cd2</th>
      <td>110</td>
      <td>3</td>
      <td>12:00</td>
    </tr>
    <tr>
      <th>5ca6c37b7a80c1d8e7abb869aff32e2e49f42192</th>
      <td>106</td>
      <td>3</td>
      <td>12:00</td>
    </tr>
    <tr>
      <th>599ef952689979428bbab00e9ece432d4b7f7eb4</th>
      <td>99</td>
      <td>3</td>
      <td>10:00</td>
    </tr>
    <tr>
      <th>14d07e4046c71d771f6a417b3f38dc8ebd6b8eab</th>
      <td>97</td>
      <td>3</td>
      <td>21:30</td>
    </tr>
    <tr>
      <th>339847a30d30a645969dff6b99208dd388e0fa7f</th>
      <td>96</td>
      <td>3</td>
      <td>17:30</td>
    </tr>
    <tr>
      <th>76c891cac696f3c35810ef38a473f1943ff071b9</th>
      <td>96</td>
      <td>5</td>
      <td>13:15</td>
    </tr>
    <tr>
      <th>496e0fdde3a9fc87a60f5aa9fc1087c2271c9faa</th>
      <td>93</td>
      <td>3</td>
      <td>12:00</td>
    </tr>
    <tr>
      <th>b3c973191e1d3a76f072a17ace6c3a1783015a4f</th>
      <td>90</td>
      <td>2</td>
      <td>14:00</td>
    </tr>
    <tr>
      <th>a207aff89060458b8370630670f2d117ae148f16</th>
      <td>90</td>
      <td>5</td>
      <td>18:45</td>
    </tr>
    <tr>
      <th>8aa7019d5c8893275171742eecf7aab305254927</th>
      <td>89</td>
      <td>4</td>
      <td>18:15</td>
    </tr>
    <tr>
      <th>def0d00c59e0d366bc017557ead362d12ca1224f</th>
      <td>89</td>
      <td>3</td>
      <td>12:00</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>6849b93b25fb080df685d6f6c12fea20dadd7a5b</th>
      <td>1</td>
      <td>0</td>
      <td></td>
    </tr>
    <tr>
      <th>2254477ab240a175c18215625c0c4cf000e88c06</th>
      <td>1</td>
      <td>0</td>
      <td></td>
    </tr>
    <tr>
      <th>540afbb4974e398a8a9dfd313dad019aaeee8de0</th>
      <td>1</td>
      <td>0</td>
      <td></td>
    </tr>
    <tr>
      <th>2266c17e29291df3ec6d934bb44619727b5c493d</th>
      <td>1</td>
      <td>0</td>
      <td></td>
    </tr>
    <tr>
      <th>261a7f9e53bfc80a5bb9f7ebb187897d6df07775</th>
      <td>1</td>
      <td>0</td>
      <td></td>
    </tr>
    <tr>
      <th>c0623ee60b9bcb1612d259c49adb09627d1492a3</th>
      <td>1</td>
      <td>0</td>
      <td></td>
    </tr>
    <tr>
      <th>58b0127aba8dd73ebac31295646ffbfe24640c0a</th>
      <td>1</td>
      <td>0</td>
      <td></td>
    </tr>
    <tr>
      <th>27933a6d3cd25f9b2c18cb0ee2d02c214516f094</th>
      <td>1</td>
      <td>0</td>
      <td></td>
    </tr>
    <tr>
      <th>822d5714422ce099eb45a571bf1ec94cfeec7c7e</th>
      <td>1</td>
      <td>0</td>
      <td></td>
    </tr>
    <tr>
      <th>b54e8d4091ff6ad44f2f0d489fbc1ab7d20232e8</th>
      <td>1</td>
      <td>0</td>
      <td></td>
    </tr>
    <tr>
      <th>a5565acadbc1703a6545b0a88d06c8c780c8b65a</th>
      <td>1</td>
      <td>0</td>
      <td></td>
    </tr>
    <tr>
      <th>881f823393959f063142ccf6cf755f1a08b28ce0</th>
      <td>1</td>
      <td>0</td>
      <td></td>
    </tr>
    <tr>
      <th>34a2415bb297d2bc5c1dc7ab67c9c7545fea5817</th>
      <td>1</td>
      <td>0</td>
      <td></td>
    </tr>
    <tr>
      <th>c5c87514bcef4d6d7d6ef98d0d6287c3b8c09e7e</th>
      <td>1</td>
      <td>0</td>
      <td></td>
    </tr>
    <tr>
      <th>36711ec670f6058f5efc6331e8497b07a32c7b40</th>
      <td>1</td>
      <td>0</td>
      <td></td>
    </tr>
    <tr>
      <th>431fc57fce5a78d074cb9d558f6d07173e8ff0cb</th>
      <td>1</td>
      <td>0</td>
      <td></td>
    </tr>
    <tr>
      <th>b3263967a4073176d252fdc51cb35174127391e1</th>
      <td>1</td>
      <td>0</td>
      <td></td>
    </tr>
    <tr>
      <th>07c9098979622bcf3d851a86c5c964b0b37aa1b7</th>
      <td>1</td>
      <td>0</td>
      <td></td>
    </tr>
    <tr>
      <th>f3f28026b179699de0fc13353ea102c86fdcc4d0</th>
      <td>1</td>
      <td>0</td>
      <td></td>
    </tr>
    <tr>
      <th>49e1e8be378cb4292ce3203967a3fd519031d47e</th>
      <td>1</td>
      <td>0</td>
      <td></td>
    </tr>
    <tr>
      <th>fd8fbed89030c680c36e3e48d3bc8e44950f68c9</th>
      <td>1</td>
      <td>0</td>
      <td></td>
    </tr>
    <tr>
      <th>c0ab26d353f7226895bd6f67f55794f62311b224</th>
      <td>1</td>
      <td>0</td>
      <td></td>
    </tr>
    <tr>
      <th>d20949b0bfd954faaf8bf9e743f7a4ee4a0157e4</th>
      <td>1</td>
      <td>0</td>
      <td></td>
    </tr>
    <tr>
      <th>77b6d8d27490c46269880cea2445303eb42e9822</th>
      <td>1</td>
      <td>0</td>
      <td></td>
    </tr>
    <tr>
      <th>a9feb20954d25796d796a711969dc3cbc2205f32</th>
      <td>1</td>
      <td>0</td>
      <td></td>
    </tr>
    <tr>
      <th>692e842c42b3e53da378ce22213e0c322208b161</th>
      <td>1</td>
      <td>0</td>
      <td></td>
    </tr>
    <tr>
      <th>38a859c35207226db069f001d6e2ec5bc2f0a534</th>
      <td>1</td>
      <td>0</td>
      <td></td>
    </tr>
    <tr>
      <th>49d59810175b84ab61b59965676ad8e4bee3e523</th>
      <td>1</td>
      <td>0</td>
      <td></td>
    </tr>
    <tr>
      <th>74ed74f512c21c86c8af21cd2320ae96f8794b86</th>
      <td>1</td>
      <td>0</td>
      <td></td>
    </tr>
    <tr>
      <th>d1224e679e4f4ca38c001895d1d7476708492a09</th>
      <td>1</td>
      <td>0</td>
      <td></td>
    </tr>
  </tbody>
</table>
<p>24024 rows × 3 columns</p>
</div>


