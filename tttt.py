# utf-8
import pandas as pd

df = pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
for i in df.index:
    print(type(i))