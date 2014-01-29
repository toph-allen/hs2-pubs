import numpy as np
from pandas import Series, DataFrame
import pandas as pd
import bs4

datadir = ('data/')
cityfile = ('geonames/cities1000.txt')

colnames = ['geonameid', 'name', 'asciiname', 'alternatenames', 'latitude', 'longitude', 'featureclass', 'featurecode', 'countrycode', 'cc2', 'admin1code', 'admin2code', 'admin3code', 'admin4code', 'population', 'elevation', 'dem', 'timezone', 'modificationdate']

cities = pd.io.parsers.read_table(datadir + cityfile, header=None, names=colnames)

# To select by the content of a variable
cities[cities.name == 'Binga']
cities.loc[cities.name == 'Binga']
cities.loc[cities.name == 'Binga', ["latitude", "longitude"]]

for row_index, row in cities.iloc[0:3].iterrows():
    print '%s\n%s' % (row_index, row)


# Reading in the paper

paperfile = 'articles/Emerg_Infect_Dis/Emerg_Infect_Dis_2005_Dec_11(12)_1887-1893.xml'
paperxml = open(datadir + paperfile, 'r')
paper = bs4.BeautifulSoup(paperxml, "lxml")

paper.find_all("front")
paper.find_all("abstract")
paper.find_all("body")


# Misc useful commands

os.getcwd()

string.count()
string.find()