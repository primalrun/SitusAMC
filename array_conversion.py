import numpy as np

arr_1d = np.array([1, 2, 3])
shape_arr_1d = arr_1d.shape
print('1d array: ',shape_arr_1d)
print('1d array shape len: ', len(shape_arr_1d))

arr_2d = np.array([[1, 2, 3],
             [4, 5, 6]])
shape_arr_2d = arr_2d.shape
print('2d array: ',shape_arr_2d)
print('2d array shape len: ', len(shape_arr_2d))

arr_3d = np.array([[[1, 2, 3]
                  ,[4, 5, 6]],
                  [[7, 8, 9],
                  [10, 11, 12]]])

shape_arr_3d = arr_3d.shape
print('3d array: ',shape_arr_3d)

arr_4d = np.array(
    [
        [[[1, 2, 3],
         [3, 4, 5]]],
        [[[7, 8, 9],
         [10, 11, 12]]],
        [[[13, 14, 15],
         [16, 17, 18]]]
    ]
)

shape_arr_4d = arr_4d.shape
print('4d array: ',shape_arr_4d)

print('\n')
arr_1d_add_row = arr_1d[np.newaxis, :]
shape_1d_add_row = arr_1d_add_row.shape
print('1d_add_row: ', shape_1d_add_row)
print(arr_1d_add_row)

print('\n')
arr_1d_add_col = arr_1d[:, np.newaxis]
shape_1d_add_col = arr_1d_add_col.shape
print('1d_add_col: ', shape_1d_add_col)
print(arr_1d_add_col)


print('\n')
