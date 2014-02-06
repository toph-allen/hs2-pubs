import os

import numpy as np
from pandas import Series, DataFrame
import pandas as pd
from bs4 import BeautifulSoup


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
