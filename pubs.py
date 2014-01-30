import os
import numpy as np
from pandas import Series, DataFrame
import pandas as pd
from bs4 import BeautifulSoup

datadir = 'data/'
articledir = datadir + 'articles/'
testdir = datadir + 'testarticles/'
geonamedir = datadir + 'geonames/'

cities1000 = 'cities1000.txt'


def get_article_paths(articledir):
    paths = []
    for (dirpath, dirs, files) in os.walk(articledir):
        for filename in files:
            reldir = os.path.relpath(dirpath)
            relfile = os.path.join(reldir, filename)
            if os.path.splitext(relfile)[1] == '.nxml':
                paths.append(relfile)
    return(paths)


def get_geoname_dataframe(geonamefile):
    colnames = ['geonameid', 'name', 'asciiname', 'alternatenames',
                'latitude', 'longitude', 'featureclass', 'featurecode',
                'countrycode', 'cc2', 'admin1code', 'admin2code',
                'admin3code', 'admin4code', 'population', 'elevation',
                'dem', 'timezone', 'modificationdate']
    dtypes = {'geonameid' : 'int64', 'name' : 'object',
              'asciiname' : 'object', 'alternatenames' : 'object',
              'latitude' : 'float64', 'longitude' : 'float64',
              'featureclass' : 'object', 'featurecode' : 'object',
              'countrycode' : 'object','cc2' : 'object',
              'admin1code' : 'object', 'admin2code' : 'object',
              'admin3code' : 'object', 'admin4code' : 'object',
              'population' : 'int64', 'elevation' : 'float64',
              'dem' : 'int64', 'timezone' : 'object',
              'modificationdate' : 'object'}
    cities = pd.io.parsers.read_table(geonamedir + geonamefile,
                                      header=None, names=colnames,
                                      dtype=dtypes, encoding='utf-8')
    return(cities)


# def match_city_names(article, )