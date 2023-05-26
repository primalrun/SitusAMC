import os

in_file = r'c:\temp\column_bracket.txt'
out_file = r'c:\temp\column_clean.txt'

with open(in_file, 'r') as r:
    lines = [str(line).strip().replace(',', '').replace('[', '').replace(']', '') for line in r]

with open(out_file, 'w') as w:
    for i, x in enumerate(lines):
        if i == 0:
            w.writelines('S.' + x)
        else:
            w.writelines('\n,S.' + x)

os.startfile(out_file)