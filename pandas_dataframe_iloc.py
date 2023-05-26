import pandas as pd

mydict = [{'a': 1, 'b': 2, 'c': 3, 'd': 4},
          {'a': 100, 'b': 200, 'c': 300, 'd': 400},
          {'a': 1000, 'b': 2000, 'c': 3000, 'd': 4000 }]

df = pd.DataFrame(mydict)

row_1 = df.iloc[0]
print('row_1')
print(row_1)
print(type(row_1))
print('\n')

int_list = df.iloc[[0]]
print('int_list')
print(int_list)
print(type(int_list))
print('\n')

slice_all = df.iloc[:, :]
print('slice_all')
print(slice_all)
print(type(slice_all))
print('\n')

int_list_2 = df.iloc[[0, 2], [1, 3]]
# the result is the same as:
# [0, 1], [0, 3]
# [2, 1], [2, 3]
print('int_list_2')
print(int_list_2)
print(type(int_list_2))
print('\n')



slice_1 = df.iloc[1:3, 0:3]
# the result is the same as:
# [1, 0], [1, 1], [1, 2]
# [2, 0], [2, 1], [2, 2]
print('slice_1')
print(slice_1)
print(type(slice_1))
print('\n')


callable_fx = df.iloc[:, lambda df: [0, 2]]
# the result is the same as:
# [0, 0], [0, 2]
# [1, 0], [1, 2]
# [2, 0], [2, 2]
print('callable_fx')
print(callable_fx)
print(type(callable_fx))
print('\n')