import os
import sys
from collections import Counter
from functools import partial
from multiprocessing import Pool, cpu_count, Lock, Process, Queue, current_process
from itertools import repeat, count, zip_longest
import time
import re

import numpy as np
from pandas import Series, DataFrame
import pandas as pd
from bs4 import BeautifulSoup

# This is basically a copy of pubs.py with the directory changed.

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
        self.get_article_meta(file)

    def get_article_meta(self, file):
        front = self.soup.find('front')
        self.meta = {}
        self.meta['filename'] = os.path.basename(file)

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
            country_name_re = r'\b' + country_name + r'\b'
            x = re.search(country_name_re, self.text)
            if x:
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
            place_name_re = r'\b' + place_name + r'\b'
            x = re.search(place_name_re, self.text)
            if x:
                self.places.append(row)
        self.places = DataFrame(self.places)

    def give_dataframe(self):
        keeps = ['geonameid', 'name', 'asciiname', 'latitude', 'longitude',
                 'population']
        try:
            export = self.places[keeps]
        except:
            return DataFrame()
        export['filename'] = self.meta['filename']
        export['row_index'] = export.index
        return(export)


class matchbox:
    def __init__(self, articlepaths):
        self.num_exports = 0
        self.num_articles_total = len(articlepaths)
        self.num_articles_matched = 0
        self.num_matches = 0
        self.dataframe = DataFrame()
        self.init_time = time.strftime("%Y-%m-%d_%H-%M-%S_")

    def update(self, matches):
        self.dataframe = self.dataframe.append(matches, ignore_index=True)
        self.num_articles_matched += 1
        self.num_matches += len(matches)
        print('Matched {} places in article {} of {} ({:.2%} complete). '
              'Total: {}.'.format(len(matches),
                                          self.num_articles_matched,
                                          self.num_articles_total,
                                          self.num_articles_matched / self.num_articles_total,
                                          self.num_matches))

    def empty_into_csv(self):
        self.num_exports += 1
        outname = outdir + self.init_time + 'laura_matches_' + str(self.num_exports) + '.csv'
        self.dataframe.to_csv(outname, encoding='utf-8')
        print('Wrote matches from chunk {} to {}.'.format(self.num_exports, outname))
        del self.dataframe
        self.dataframe = DataFrame()


def chunker(iterable, n, fillvalue='STOP'):
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
            if os.path.splitext(relfile)[1] == '.html':
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


def match_places_in_article(paths_queue, matches_queue):
    for args in iter(paths_queue.get, 'STOP'):
        path, places, countries, tag = args
        x = article(path)
        x.get_tag_text(tag)
        x.match_countries(countries)
        x.match_places(places)
        matches_queue.put(x.give_dataframe())


def main():
    from math import ceil
    import gc
    import datetime

    # Set up our paths and stuff.
    articlepaths = get_article_paths('/Users/toph/Dropbox (EcoHealth Alliance)/repositories/hotspots2-pubs/laura_out/')
    places = get_geoname_dataframe('cities1000.txt')
    countries = get_countryinfo_dataframe('countryInfo.txt')
    tag = 'p'

    # Prepare to iterate
    chunk_size = 50
    chunks = chunker(articlepaths, chunk_size)
    numchunks = ceil(len(articlepaths)/chunk_size)

    print('About to start matching places from {} articles in chunks of {}.\n'
          'That\'s {} chunks.'.format(len(articlepaths), chunk_size, numchunks))

    allmatches = matchbox(articlepaths)

    t1 = t1 = datetime.datetime.now()

    for chunk in chunks:

        args = zip(chunk, repeat(places), repeat(countries),
                      repeat(tag))

        # Create queues
        paths_queue = Queue()
        matches_queue = Queue()

        num_tasks = 0

        # Submit tasks
        for arg in args:
            if arg[0] != 'STOP':
                paths_queue.put(arg)
                num_tasks += 1

        # Start worker processes
        workers = 8
        for i in range(workers):
            Process(target=match_places_in_article,
                    args=(paths_queue, matches_queue)).start()

        # Process results
        for i in range(num_tasks):
            allmatches.update(matches_queue.get())

        for i in range(workers):
            paths_queue.put('STOP')

        allmatches.empty_into_csv()
    t2 = datetime.datetime.now()
    print(t2 - t1)






if __name__ == '__main__':
    main()
