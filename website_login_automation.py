from selenium import webdriver
import yaml

conf = yaml.load(open('login_details.yml'))
gnma_email = conf['gnma_user']['email']
gnma_password = conf['gnma_user']['password']