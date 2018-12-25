
# Send Time Optimization
Susumu Asaga

Iniciamos com as importações padrões.


```python
%matplotlib inline
import matplotlib.pyplot as plt
import seaborn as sns; sns.set()
import numpy as np
import pandas as pd
```

Utilizaremos o KDE do pacote scikit-learn.

```python
from sklearn.neighbors import KernelDensity
```

Vamos fazer uma estimativa para cada um dos 1440 minutos das 24 horas do dia da semana corrente.


```python
x_d = np.linspace(0, 24, 1441)
```

Foram preparados sete conjuntos de dados com as horas dos eventos `open` em arquivos textos, um para cada dia da semana. Para cada dia da semana, faremos a leitura do arquivo e a estimativa de densidade usando o kernel de Epanechnikov e largura de banda igual a 1.


```python
week_day='saturday'
table = pd.read_csv(week_day + '.csv')
x = table['time'].values
kde = KernelDensity(bandwidth=1, kernel='epanechnikov')
kde.fit(x[:, None])
logprob = kde.score_samples(x_d[:, None])
y_d = np.exp(logprob)
fig, ax = plt.subplots()
fig.set_size_inches(12, 9)
ax.set(xlim=(0, 24), xlabel='time', ylabel='density', title=week_day)
ax.xaxis.set_major_locator(plt.MultipleLocator(1))
ax.grid(which='major', axis='x', linestyle='-')
ax.plot(x_d,  y_d)
plt.savefig('pde_' + week_day + '.png')
pde= pd.DataFrame(data={ 'time': x_d, 'density': y_d })
pde.to_csv('pde_' + week_day + '.csv')
```
