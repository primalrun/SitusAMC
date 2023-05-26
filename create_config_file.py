from configparser import ConfigParser
import os

# variable
config_file = 'config.ini'

# get the configparser object
config_object = ConfigParser()

config_object['DW_PROD'] = {
    'server_name': r'AMC-DLKDWP01.AMCFIRST.COM'
    , 'ip_address': r'10.22.53.237'
    , 'ssa_id': ''
    , 'ssa_password': ''
    , 'port': ''
}

config_object['DW_DEV'] = {
    'server_name': r'COM-DLKDWD01.SITUSAMC.COM'
    , 'ip_address': r'10.24.21.11'
    , 'ssa_id': ''
    , 'ssa_password': ''
    , 'port': ''
}

config_object['DL_PROD'] = {
    'server_name': r'AMC-DLKRPTP01.AMCFIRST.COM'
    , 'ip_address': r'10.22.53.234'
    , 'ssa_id': ''
    , 'ssa_password': ''
    , 'port': ''
}

# remove old config file
try:
    os.remove(config_file)
except:
    print('error while deleting file ', config_file)

# create config file
with open(config_file, 'w') as conf:
    config_object.write(conf)

print('config file is created')
