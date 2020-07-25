# utf-8
import pandas as pd

temp_df = pd.DataFrame(columns=('a', 'b', 'c'),
                       data=[['a', 'a', 'c'],
                             ['a', 'a', 'b'],
                             ['c', 'b', 'c'],
                             ['e', 'g', ]])
print(pd.isna(temp_df.loc[3,'c']))

