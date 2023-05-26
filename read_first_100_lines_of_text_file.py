from itertools import islice

in_file = r'C:\Users\jasonwalker\OneDrive - SitusAMC\Documents\project\baseline\source_file\202112\GNMA_HMB_LL_MON_rl2_202112_001.txt'

with open(in_file) as f:
    for line in islice(f, 100):
        print(line)






