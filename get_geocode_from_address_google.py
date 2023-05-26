# https://stackoverflow.com/questions/67851013/scraping-longitude-and-latitude-of-a-city-from-google-maps

import pandas as pd
import time
import os
from bs4 import BeautifulSoup as soup
import requests
import re

out_file = r'c:\temp\out1.xlsx'

addresses = [
    '1759 CALLE ALFONSO TORO, CDMX, 09060'
    # ,'2401 COLORADO AVENUE SANTA MONICA, CALIFORNIA 90401'
    # ,'9485 CUSTOMHOUSE PLAZA SAN DIEGO, CALIFORNIA 92154'
    # ,'2200 NORTHMONT PARKWAY DULUTH, GEORGIA 30096'
    # ,'2121 CLAYTON ROAD CLAYTON, INDIANA 46118'
    # ,'3112 HORSESHOE LANE CHARLOTTE, NORTH CAROLINA 28208'
    # ,'3112 HORSESHOE LANE CHARLOTTE, NORTH CAROLINA 28208'
    # ,'3112 HORSESHOE LANE CHARLOTTE, NORTH CAROLINA 28208'
    # ,'1001 WESTHEIMER ROAD HOUSTON, TEXAS 77006'
    # ,'5701 BUSINESS PARK SAN ANTONIO, TEXAS 78218'
    # ,'4703 GREATLAND DRIVE SAN ANTONIO, TEXAS 78218'
    # ,'1901 SHIPMAN DRIVE SAN ANTONIO, TEXAS 78219'
    # ,'1910 SHIPMAN DRIVE SAN ANTONIO, TEXAS 78219'
    # ,'3439 STEEN STREET SAN ANTONIO, TEXAS 78219'
    # ,'1800 SHIPMAN DRIVE SAN ANTONIO, TEXAS 78219'
    # ,'3415 STEEN STREET SAN ANTONIO, TEXAS 78219'
    # ,'4615 GREATLAND DRIVE SAN ANTONIO, TEXAS 78218'
    # ,'1946 SHIPMAN DRIVE SAN ANTONIO, TEXAS 78219'
]


for a in addresses:
    google_address = a.replace(' ', '+')
    url = f'http://www.google.cl/maps/place/{google_address}'
    response = requests.request(method='GET', url=url)
    soup_parser = soup(response.text, 'html.parser')
    html_content = soup_parser.html.contents[1]
    # html_1 = html_content[0]


    print(html_content)
    print(type(html_content))
