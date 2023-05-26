t1 = ['a', 'b', 'c', 'b']
gen_error_message = 'Last Generator index has been reached'

gen = (x[0] for i, x in enumerate(t1) if x[0] == 'b')
print(gen)
print('\n')

n1 = next(gen, gen_error_message)
n2 = next(gen, gen_error_message)
n3 = next(gen, gen_error_message)

print(n1, n2, n3, sep='\n')



