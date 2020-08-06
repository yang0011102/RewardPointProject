# utf-8

import pandas as pd


class IterIndexDataFrame(pd.DataFrame):
    def __iter__(self):
        for _index in self.index:
            yield self.loc[_index, :]


ttdf = pd.DataFrame([[1, 2, 3],
                     ['a', 'b', 'c'],
                     ['4', 'hh', 6]], columns=('c1', 'c2', 'c3'))
fff = IterIndexDataFrame([[1, 2, 3],
                          ['a', 'b', 'c'],
                          ['4', 'hh', 6]], columns=('c1', 'c2', 'c3'))

a=['']
b="aaaaa : {0[0]}"
print(b.format(a),'\n',b)