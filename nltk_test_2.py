from math import ceil
import gc
import datetime

import nltk
import re

from pubs import *

# Set up our paths. We're using the larger test set of articles.
articlepaths = get_article_paths(articledir)
articlepaths = articlepaths[899:999]
places = get_geoname_dataframe('cities1000.txt')
countries = get_countryinfo_dataframe('countryInfo.txt')
tag = 'p'


# This script updates the pubs article class in a few ways.
# In particular, when matching country and place names in article, it uses
# regex matching to look only for full-word matches.

class article2(article):
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


# We also have to update the match_places_in_article function.

def match_places_in_article(paths_queue, matches_queue):
    for args in iter(paths_queue.get, 'STOP'):
        path, places, countries, tag = args
        x = article2(path) # For this
        x.get_tag_text(tag)
        x.match_countries(countries)
        x.match_places(places)
        matches_queue.put(x.give_dataframe())


# Prepare to iterate
chunk_size = 10000
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
    workers = 2
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