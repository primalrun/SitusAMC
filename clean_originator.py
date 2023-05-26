import pandas as pd
import os

source_file = r'c:\temp\ORIGINATOR_FULL.txt'
out_file = r'c:\temp\ORIGINATOR_CLEAN.csv'
suffix_search = (
    'LLC'
    ,'CORP'
    ,'INC'
    ,'CO'
    ,'L L C '
)


def clean_data(text):
    text_s = str(text).strip()
    if text_s.rfind(' ') == -1:
        # no space in string
        return text_s
    else:
        str_suffix = text_s[text_s.rfind(' ') + 1:]
        if str_suffix in suffix_search:
            str_prefix = text_s[:text_s.rfind(' ')]
            return str_prefix
        else:
            return text_s



df_source = pd.read_csv(filepath_or_buffer=source_file, names=['SOURCE'])
df_clean = df_source.copy()
df_clean['SOURCE_CLEAN'] = df_clean['SOURCE'].apply(lambda x: clean_data(x))

df_clean.to_csv(path_or_buf=out_file, index=False)
os.startfile(out_file)
print('success')
