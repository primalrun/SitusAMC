import os

source_dir = r'C:\Users\jasonwalker\OneDrive - SitusAMC\Documents\project\CREST_UNIVERSAL\DEPLOY'
extension = '.sql'
out_file = r':c:\temp\deploy_sql_files.txt'

deploy_files = [file for file in os.listdir(source_dir) if file.endswith(extension)]

for f in deploy_files:
    print(f)


