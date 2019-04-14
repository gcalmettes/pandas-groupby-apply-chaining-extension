# pandas-groupby-extension

This library register a [custom accessor](https://pandas.pydata.org/pandas-docs/stable/development/extending.html) on [Pandas](https://pandas.pydata.org) DataFrame and Series that allows to construct independant pipelines of functions to be applied independently on the groups of a Pandas groupby object.

In this way, `.apply` methods can be chained.

The methods of this library can be accessed using the `gc` namespace.

```
import numpy as np
import pandas as pd

import gcGroupbyExtension

df = pd.DataFrame({
    "col1": np.random.rand(10),
    "col2": np.random.rand(10),
    "col3": np.random.rand(10),
    "col4": [*np.ones(6), *np.zeros(4)]
})

def normalize(df, columns=[0, 1, 2]):
    out = df.copy()
    temp = df.iloc[:, columns]
    temp = (temp - temp.mean())/temp.std()
    out.iloc[:, columns] = temp
    return out

df.gc.groupby("col4")\
    .resetIndex()\
    .apply(lambda x: x * 5)\
    .apply(normalize)\
    .apply(lambda x: x - np.concatenate([x.iloc[0, :-1].values, [0]]))\
    .apply(lambda x: x+3, lambda x: x/2)\
    .concat()
```
