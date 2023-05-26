import pyperclip
import sys
import os

out_file = r'c:\temp\table_column.txt'

clipboard_data = pyperclip.paste()
if clipboard_data is None:
    print('Process Cancelled, Clipboard is empty')
    sys.exit()

column_split = clipboard_data.split('\r\n')
column_name = [x.split(' ')[0] for x in column_split]

with open(out_file, 'w') as f:
    for x in column_name:
        f.write(x + '\n')

os.startfile(out_file)
