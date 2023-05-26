from itertools import islice

in_file = r'C:\Users\jasonwalker\OneDrive - SitusAMC\Documents\project\baseline\source_file\hmbs_portfolio' \
          r'\hllmon1_202111\GNMA_HMB_LL_MON_rl2_202111_001.txt'

with open(in_file) as f:
    for line in islice(f, 100):
        print(line)
        print(type(line))





