import pandas as pd

# variable
commission_rate = .05

# dataframe data, dictionary of lists
data = {'year': [2019, 2020, 2021, 2021],
        'quantity': [5, 10, 15, None],
        'sales_price': [1, 2, 3, None],
        'product': ['software', 'software', 'service', 'service']
        }


# function for commission rate
def commission_calc(quantity, sales_price):
    if quantity < 10:
        return sales_price * quantity * commission_rate
    else:
        return sales_price * quantity * (commission_rate * 2)


# create dataframe
df1 = pd.DataFrame(data=data)

# create commission calculation column with apply and commission_calc function
df1['commission'] = df1.apply(lambda x: commission_calc(x['quantity'], x['sales_price']), axis=1)

# print df1 and test commission_calc result
print('df1 and test commission_calc result', '\n')
print(df1)
print('commission test', 5 * commission_rate)
print('commission test', 20 * (commission_rate * 2))
print('commission test', 45 * (commission_rate * 2))
print('\n')



# create new dataframe for software product only using apply
df_software = df1[df1.apply(lambda x: x['product'] == 'software', axis=1)]

df_software_2 = df1[df1['product'] == 'software']
print('df_software', '\n')
print(df_software_2)
print('\n')


# add column for color blue
df1.insert(loc=1, column='color', value='blue')
# inserts new column color before column 1 based on 0 index column


# sort df1 by product, year
df1 = df1.sort_values(by=['product', 'year'])
print('df1 sorted by product and year', '\n')
print(df1, '\n')

# replace nan values with 0
df_no_null = df1.fillna(0)
print('df_no_null, replaced null with 0', '\n')
print(df_no_null, '\n')
