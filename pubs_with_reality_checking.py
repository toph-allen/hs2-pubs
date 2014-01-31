import os
import numpy as np
from pandas import Series, DataFrame
import pandas as pd
from bs4 import BeautifulSoup
from collections import Counter
from sys import argv

datadir = 'data/'
articledir = datadir + 'articles/'
testdir = datadir + 'testarticles/'
geonamedir = datadir + 'geonames/'
outdir = 'out/'

cities1000 = 'cities1000.txt'
countryfile = 'countryInfo.txt'


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
    places = pd.io.parsers.read_table(geonamedir + geonamefile,
                                      header=None, names=colnames,
                                      dtype=dtypes, encoding='utf-8')
    return(places)


def get_countryinfo_dataframe(countryfile):
    countries = pd.io.parsers.read_table(geonamedir + countryfile, encoding='utf-8')
    return(countries)


def get_matches(path, places, tag):
    """Given an article, a list of places and a tag name. Searches the
    text of the given tag for matches of places in the article text.
    Returns a list of row indices for the given 'places' DataFrame."""

    articlesoup = BeautifulSoup(open(path, 'r'), 'lxml')

    tags = articlesoup.find_all(tag)
    text = ''

    for tag in tags:
        tagtext = tag.get_text() + '\n\n'
        text += tagtext   

    matches = [] 

    for row_index, row in places.iterrows():
        placename = row.loc['name']
        count = text.count(placename)
        if count > 0:
            print 'Found %i entries for place %s.' %(count, placename)
            matches.append(row_index)

    return(matches)


def get_all_matches(articlepaths, places, tag):
    allmatches = []
    for path in articlepaths:
        print 'Working on %s.' %path
        matches = get_matches(path, places, tag)
        allmatches.extend(matches)
    return(allmatches)


def count_all_matches(articlepaths, places, tag):
    allmatches = get_all_matches(articlepaths, places, tag)
    counts = Counter(allmatches)
    counts = Series(counts)
    counts = DataFrame(counts)
    counts.columns = ["count"]
    return(counts)


def main():
    articledir, geonamefile, tag = argv
    articlepaths = get_article_paths(articledir)
    places = get_geoname_dataframe(geonamefile)
    countries = get_countryinfo_dataframe(countryfile)
    counts = count_all_matches(articlepaths, places, tag)
    joined = counts.join(places)
    matches.to_csv(outdir + 'matched_places.csv', encoding = 'utf-8')


if __name__ == '__main__':
    main()