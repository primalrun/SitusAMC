from itertools import islice

in_file = r'C:\Users\jasonwalker\OneDrive - SitusAMC\Documents\project\baseline\history_file\vectors_december_small.csv'
out_file = r'C:\Users\jasonwalker\OneDrive - SitusAMC\Documents\project\baseline\history_file\vectors_test.csv'

with open(in_file) as r, open(out_file, 'w') as w:
    for line in islice(r, 100):
        w.write(line)









