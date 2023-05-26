import os
import datetime

file_dir = r'C:\Users\jasonwalker\OneDrive - SitusAMC\Documents\project\baseline\documentation\archive'
date_current = datetime.datetime.now()
date_string = datetime.datetime.strftime(date_current, '%Y%m%d_%H%M%S')

for file in os.listdir(file_dir):
    file_current = os.path.join(file_dir, file)
    file_name_only, file_extension = os.path.splitext(file)
    file_name_new = file_name_only + '_' + date_string + file_extension
    file_new = os.path.join(file_dir, file_name_new)
    os.rename(file_current, file_new)

print('Success')



