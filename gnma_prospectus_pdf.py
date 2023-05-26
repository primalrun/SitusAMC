from PyPDF2 import PdfReader
import sys
import os
import datetime
import pandas as pd

# in_file = r'C:\Users\jasonwalker\OneDrive - SitusAMC\Documents\project\baseline\stand_alone_file\cmo_expanded\BRIDGET_FILES_202212\2022Dec23-026O.pdf'
source_dir = r'C:\Users\jasonwalker\OneDrive - SitusAMC\Documents\project\baseline\stand_alone_file\cmo_expanded\PDF_PROSPECTUS'


def clean_period_and_space(string):
    first_period_index = string.index('.')
    string = string[:first_period_index]
    return ''.join([s for s in string if s != ' '])


def clean_tranche(row):
    if '(' in row:
        if row.index('(') == 2:
            tranche = row[:row.index('(')].split()[0]
            return tranche
        else:
            return clean_period_and_space(row)
    else:
        return clean_period_and_space(row)


def find_page_with_search_text(pages, search_text, page_range=[]):
    if len(page_range) > 0:
        for r in page_range:
            page_iter = pages[r]
            page_text = page_iter.extract_text()
            if search_text in page_text:
                return r
    else:
        for x in range(0, len(pages)):
            page_iter = pages[x]
            page_text = page_iter.extract_text()
            if search_text in page_text:
                return x
    return 9999


def clean_date_string(date_string):
    date_iter = date_string
    while date_iter[-1].isdigit() is False:
        date_iter = date_iter[:-1]
    return date_iter


in_files = [os.path.join(source_dir, file) for file in os.listdir(source_dir) if file.endswith('.pdf')]

group_tranche = []
group_tranche_mx = []

# loop through all pdf files
for file_iter in in_files:
    cmo = file_iter.split('\\')[-1]  # file name only, last section of file path
    cmo = cmo.split('.')[0]  # remove file extension
    cmo = cmo[:4] + '-' + 'H' + cmo[-4:-1]

    with open(file_iter, 'rb') as f:
        pdf_reader = PdfReader(f)
        pages = pdf_reader.pages

        # lead, issue date, group and tranche
        first_page_index = find_page_with_search_text(
            pages=pages, search_text='Offering Circular Supplement', page_range=[0, 1, 2])
        if first_page_index == 9999:
            print('Could not find Offering Circular Supplement on first few pages, process cancelled')
            sys.exit()

        page_text = pages[first_page_index].extract_text()

        issue_date_index = max(
            [i for i, r in enumerate(page_text.splitlines())
             if 'The date of this Offering Circular Supplement' in r]
        )

        lead_date = page_text.splitlines()[issue_date_index - 2:issue_date_index + 1]
        lead_date = [x for x in lead_date if len(x) > 2
                     and '1934' not in x
                     and 'Terms Sheet' not in x]
        lead = lead_date[0].split(' ')[0]
        issue_date = lead_date[1].split('The date of this Offering Circular Supplement is ')[1]
        issue_date = issue_date.replace('.', '')
        # trim last character(s) if not a number, looking for year text ending in number
        issue_date = clean_date_string(issue_date)
        issue_date = datetime.datetime.strptime(issue_date, '%B %d, %Y')
        issue_date = datetime.date.strftime(issue_date, '%#m/%#d/%Y')

        tranche_rows = [r for r in page_text.splitlines()
                        if '........' in r
                        or r[:8] == 'Security']

        for x in tranche_rows:
            print(x)


        security_group_max_index = max([i for i, r in enumerate(tranche_rows) if r[:8] == 'Security'])
        last_security_index_add = min(
            [i for i, r in enumerate(tranche_rows[security_group_max_index + 1:]) if r[10] != '.'])
        tranche_rows = tranche_rows[:security_group_max_index + last_security_index_add + 1]

        tranche_clean = []
        for r in tranche_rows:
            if r[:8] == 'Security':
                group = min([int(s) for s in r.split() if s.isdigit()])
            else:
                tranche = clean_tranche(r)
                tranche_clean.append([group, tranche])

        mx_tranche_clean = []
        mx_page_index = find_page_with_search_text(
            pages=pages, search_text=r'Available Combinations(')

        if mx_page_index != 9999:
            page_text = pages[mx_page_index].extract_text()
            mx_tranche_rows = [r for r in page_text.splitlines()
                               if r[2] == ' ']
            for r in mx_tranche_rows:
                combination_tranche = r[:2]
                if len(r) > 30:
                    row_split = r[2:].split(' ')
                    tranche = max([r for r in row_split if len(r) == 2])
                mx_tranche_clean.append([tranche, combination_tranche])

        if len(tranche_clean) > 0:
            for x in range(0, len(tranche_clean)):
                insert_iter = [
                    cmo
                    , tranche_clean[x][0]
                    , tranche_clean[x][1]
                    , lead
                    , issue_date
                               ]
                group_tranche.append(insert_iter)

        if len(mx_tranche_clean) > 0:
            for x in range(0, len(mx_tranche_clean)):
                insert_iter = [
                    cmo
                    , 'MX'
                    , mx_tranche_clean[x][0]
                    , mx_tranche_clean[x][1]
                    , lead
                    , issue_date
                               ]
                group_tranche_mx.append(insert_iter)


if len(group_tranche) > 0:
    data = group_tranche
    col = ['CMO', 'GroupNumber', 'Tranche', 'Lead', 'IssueDate']
    df = pd.DataFrame(data=data, columns=col)
    print(df)




