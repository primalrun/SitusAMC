import pandas as pd

left = pd.DataFrame(
    {
        "key1": ["K0", "K0", "K1", "K2"],
        "key2": ["K0", "K1", "K0", "K1"],
        "A": ["A0", "A1", "A2", "A3"],
        "B": ["B0", "B1", "B2", "B3"],
    }
)

right = pd.DataFrame(
    {
        "key1": ["K0", "K1", "K1", "K2"],
        "key2": ["K0", "K0", "K0", "K0"],
        "C": ["C0", "C1", "C2", "C3"],
        "D": ["D0", "D1", "D2", "D3"],
    }
)

merge_inner = pd.merge(left, right, how='inner', on=['key1', 'key2'], suffixes=('_l', '_r'))
print('merge_inner', '\n', merge_inner, '\n')

merge_left = pd.merge(left, right, how='left', on=['key1', 'key2'], suffixes=('_l', '_r'))
print('merge_left', '\n', merge_left, '\n')

merge_right = pd.merge(left, right, how='right', on=['key1', 'key2'], suffixes=('_l', '_r'))
print('merge_right', '\n', merge_right, '\n')

merge_outer = pd.merge(left, right, how='outer', on=['key1', 'key2'], suffixes=('_l', '_r'))
print('merge_outer', '\n', merge_outer, '\n')

merge_cross = pd.merge(left, right, how='cross', suffixes=('_l', '_r'))
print('merge_cross', '\n', merge_cross, '\n')

# single column from dataframe
merge_cross_key1_left = merge_cross.key1_l
print('merge_cross_key1_left', '\n', merge_cross_key1_left, '\n')
print(type(merge_cross_key1_left))



