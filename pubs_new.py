import os
from sys import argv
from collections import Counter

import numpy as np
from pandas import Series, DataFrame
import pandas as pd
from bs4 import BeautifulSoup

datadir = 'data/'
articledir = datadir + 'articles/'
testdir = datadir + 'testarticles/'
geonamedir = datadir + 'geonames/'
outdir = 'out/'
pubmeddir = 'pubmed/'

cities1000 = 'cities1000.txt'
countryInfo = 'countryInfo.txt'


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
    dtypes = {'geonameid': 'int64', 'name': 'object',
              'asciiname': 'object', 'alternatenames': 'object',
              'latitude': 'float64', 'longitude': 'float64',
              'featureclass': 'object', 'featurecode': 'object',
              'countrycode': 'object', 'cc2': 'object',
              'admin1code': 'object', 'admin2code': 'object',
              'admin3code': 'object', 'admin4code': 'object',
              'population': 'int64', 'elevation': 'float64',
              'dem': 'int64', 'timezone': 'object',
              'modificationdate': 'object'}
    places = pd.io.parsers.read_table(geonamedir + geonamefile,
                                      header=None, names=colnames,
                                      dtype=dtypes, encoding='utf-8')
    return(places)


def get_countryinfo_dataframe(countryfile):
    countries = pd.io.parsers.read_table(geonamedir + countryfile,
                                         encoding='utf-8')
    return(countries)


def get_country_matches(text, countries, tag):
    """Given an article, the list of countries and a tag name, searches
    the text of the given tags for matches of country.
    """
    matches = []
    for row_index, row in countries.iterrows():
        countryname = row.loc['Country']
        count = text.count(countryname)
        if count > 0:
            # print 'Matched country %s.' %(count, countryname)
            matches.append(row.loc['ISO'])
    return(matches)


def get_place_matches(text, places, tag):
    """Given an article, a list of places and a tag name, searches the
    text of the given tags for matches of places in the article text.
    Returns a list of row indices for the given 'places' DataFrame."""
    matches = []
    for row_index, row in places.iterrows():
        placename = row.loc['name']
        count = text.count(placename)
        if count > 0:
            matches.append(row_index)
    return(matches)


def get_all_matches(articlepaths, places, countries, tag):
    allmatches = DataFrame()
    n = len(articlepaths)

    for i, path in enumerate(articlepaths):
        print 'Working on article %i of %i...' % (i + 1, n)

        article = open(path, 'r')
        articlesoup = BeautifulSoup(article, 'lxml')
        article.close()

        tags = articlesoup.find_all(tag)
        text = ''

        for t in tags:
            tagtext = t.get_text() + '\n\n'
            text += tagtext

        countrymatches = get_country_matches(text, countries, tag)
        criterion = places['countrycode'].map(lambda x: x in countrymatches)

        matches = get_place_matches(text, places[criterion], tag)
        print 'Matched %i places in %i countries.' % (len(matches),
                                                      len(countrymatches))

        matches = DataFrame({'row_index': matches})

        # Originally, wanted to put the publication date for 'ppub' but not
        # all articles have that.

        try:
            matches['year'] = int(articlesoup.front.find(name='pub-date', attrs={'pub-type': 'ppub'}).year.get_text())
        except:
            matches['year'] = int(articlesoup.front.find(name='pub-date').year.get_text())

        try:
            matches['journal'] = articlesoup.front.find('journal-title').get_text()
        except:
            pass

        try:
            matches['doi'] = articlesoup.front.find(name='article-id', attrs={'pub-id-type': 'doi'}).get_text()
        except:
            pass

        try:
            matches['pmid'] = articlesoup.front.find(name='article-id', attrs={'pub-id-type': 'pmid'}).get_text()
        except:
            pass

        allmatches = allmatches.append(matches, ignore_index=True)

        print 'Total matches: %i' % len(allmatches.index)
    return(allmatches)


# def count_all_matches(articlepaths, places, countries, tag):
#     allmatches = get_all_matches(articlepaths, places, countries, tag)
#     counts = Counter(allmatches)
#     counts = Series(counts)
#     counts = DataFrame(counts)
#     counts.columns = ["count"]
#     return(counts)


def main():
    articledir, geonamefile, tag = argv
    articlepaths = get_article_paths(articledir)
    places = get_geoname_dataframe(geonamefile)
    countries = get_countryinfo_dataframe(countryfile)
    # counts = count_all_matches(articlepaths, places, countries, tag)
    # joined = counts.join(places)
    matches.to_csv(outdir + 'matched_places.csv', encoding='utf-8')


if __name__ == '__main__':
    main()
