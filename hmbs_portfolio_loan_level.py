

source_file = r'C:\Users\jasonwalker\OneDrive - SitusAMC\Documents\project\baseline\source_file\GNMA_HMB_LL_MON_rl2_202111_001.txt'
count = 0

with open(source_file, 'r') as f:
    for line in f:
        if line[0] == 'L':
            count += 1
print(count)
