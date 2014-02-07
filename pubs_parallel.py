import os
import sys
from collections import Counter
from functools import partial
from multiprocessing import Pool, cpu_count
from itertools import repeat, count, zip_longest

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


class article:
    def __init__(self, file):
        # We initialize the article with a serving of XML soup.
        self.soup = BeautifulSoup(open(file), 'lxml')
        self.get_article_meta()

    def get_article_meta(self):
        front = self.soup.find('front')
        self.meta = {}
        try:
            self.meta['year'] = int(front.find('pub-date').year.get_text())
            self.meta['pub_type'] = front.find('pub-date').attrs['pub-type']
        except:
            self.meta['year'] = None
            self.meta['pub-type'] = None
        try:
            self.meta['journal'] = front.find('journal-title').get_text()
        except:
            self.meta['journal'] = None
        try:
            self.meta['doi'] = front.find(name='article-id', attrs={'pub-id-type': 'doi'}).get_text()
        except:
            self.meta['doi'] = None
        try:
            self.meta['pmid'] = front.find(name='article-id', attrs={'pub-id-type': 'pmid'}).get_text()
        except:
            self.meta['pmid'] = None

    def get_tag_text(self, tag):
        self.text = ''
        # Find all of the specified instances of the given tag.
        all_tags = self.soup.find_all(tag)
        # Iterate through them and extract the text.
        for t in all_tags:
            tag_text = t.get_text() + '\n\n'
            self.text += tag_text

    def match_countries(self, all_countries):
        self.countries = []
        # We iterate through the all_countries data frame. If it appears in
        # the text, we append its ISO code to the 'countries' list.
        for row_index, row in all_countries.iterrows():
            country_name = row.loc['Country']
            count = self.text.count(country_name)
            if count > 0:
                self.countries.append(row.loc['ISO'])

    def match_places(self, all_places):
        self.places = []
        # We iterate through the all_places data frame. If a place appears in
        # the text, we append its row_index to the self.places list? No,
        # we're just going to append its row to the data frame.
        # We only match places that are in matched countries.
        keep = all_places['countrycode'].map(lambda x: x in self.countries)
        for row_index, row in all_places[keep].iterrows():
            place_name = row.loc['name']
            count = self.text.count(place_name)
            if count > 0:
                self.places.append(row)
        self.places = DataFrame(self.places)

    def give_dataframe(self):
        keeps = ['geonameid', 'name', 'asciiname', 'latitude', 'longitude',
                 'population']
        try:
            export = self.places[keeps]
        except:
            return DataFrame()
        export['year'] = self.meta['year']
        export['pub_type'] = self.meta['pub_type']
        export['journal'] = self.meta['journal']
        export['doi'] = self.meta['doi']
        export['pmid'] = self.meta['pmid']
        export['row_index'] = export.index
        return(export)


class matchbox:
    def __init__(self, articlepaths):
        self.num_exports = 0
        self.num_articles_total = len(articlepaths)
        self.num_articles_matched = 0
        self.num_matches = 0
        self.dataframe = DataFrame()

    def update(self, matches):
        print('Update called.')
        self.dataframe = self.dataframe.append(matches, ignore_index=True)
        self.num_articles_matched += 1
        self.num_matches += len(matches)
        print('Matched {} places in article {} of {} ({:.2%} complete). '
              'Total matches: {}.'.format(len(matches),
                                          self.num_articles_matched,
                                          self.num_articles_total,
                                          self.num_articles_matched / self.num_articles_total,
                                          self.num_matches))

    def empty_into_csv(self):
        self.num_exports += 1
        outname = outdir + 'test_export_' + str(self.num_exports) + '.csv'
        self.dataframe.to_csv(outname, encoding='utf-8')
        del self.dataframe
        self.dataframe = DataFrame()


def chunker(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # chunker('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


def build_test_paths():
    articlepaths = get_article_paths(testdir)
    places = get_geoname_dataframe(cities1000)
    countries = get_countryinfo_dataframe(countryInfo)
    return articlepaths, places, countries


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


def match_places_in_article(path, places, countries, tag):
    x = article(path)
    print('Matching places in article {}.'.format(x.meta['doi']))
    x.get_tag_text(tag)
    x.match_countries(countries)
    x.match_places(places)
    return(x.give_dataframe())


# def get_all_matches(articlepaths, places, countries, tag):
#     allmatches = DataFrame()
#     n = len(articlepaths)
#     for i, path in enumerate(articlepaths):
#         matches = match_places_in_article(path, places, countries, tag)
#         allmatches = allmatches.append(matches, ignore_index=True)
#         print(('Matches in article %i of %i: %i.'
#               ' Total matches: %i' % (i+1, n, len(matches), len(allmatches))))
#     return(allmatches)


# def get_all_matches_parallel(articlepaths, places, countries, tag):
#     pool = Pool(processes=4)
#     allmatches = DataFrame()
#     n = len(articlepaths)
#     partial_match_places_in_article = partial(match_places_in_article,
#                                                  places=places,
#                                                  countries=countries,
#                                                  tag=tag)
#     matches_list = pool.map(partial_match_places_in_article, articlepaths)
#     allmatches = pd.concat(matches_list)
#     return(allmatches)


# def get_all_matches_starmap(articlepaths, places, countries, tag,
#                             processes=int(cpu_count()/2)):
#     pool = Pool(processes=processes)
#     allmatches = DataFrame()
#     n = len(articlepaths)
#     pathzip = zip(articlepaths, repeat(places), repeat(countries),
#                   repeat(tag), count(1), repeat(n))
#     matches_list = pool.starmap(match_places_in_article, pathzip,
#                                 chunk_size=10)
#     pool.close()
#     allmatches = pd.concat(matches_list)
#     return(allmatches)

# def count_all_matches(articlepaths, places, countries, tag):
#     
#     counts = Counter(allmatches)
#     counts = Series(counts)
#     counts = DataFrame(counts)
#     counts.columns = ["count"]
#     return(counts)


def main():
    import datetime
    from math import ceil
    import gc

    articlepaths = get_article_paths('/Users/toph/PMC Articles/')
    places = get_geoname_dataframe('cities1000.txt')
    countries = get_countryinfo_dataframe('countryInfo.txt')
    tag = 'p'

    chunk_size = 10
    chunks = chunker(articlepaths, chunk_size)
    numchunks = ceil(len(articlepaths)/chunk_size)

    allmatches = matchbox(articlepaths)

    for c, chunk in enumerate(chunks):
        print(len(chunk))
        print('Starting work on chunk {} of {}.'.format(c + 1, numchunks))
        print
        pool = Pool(processes=8)
        args = zip(chunk, repeat(places), repeat(countries),
                      repeat(tag))
        m = pool.starmap_async(match_places_in_article, args, callback=allmatches.update)
        m.wait()
        allmatches.empty_into_csv()

        pool.close()
        del pool


if __name__ == '__main__':
    main()
