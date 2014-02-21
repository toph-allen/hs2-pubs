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

# This script uses NLTK's regular expression chunker to flag noun phrases
# in the document and matche country and place names to those.

class article_nltk(article):
    
    def pos_tag_words(self):
        cleaned = re.sub('[()]', '', self.text)
        words = nltk.word_tokenize(cleaned)
        self.tagged_words = nltk.pos_tag(words)
    
    def candidates_with_ne_chunk(self):
        self.chunked = nltk.ne_chunk(self.tagged_words)
        
        self.candidates = []
        for n in self.chunked:
            if isinstance(n, nltk.tree.Tree) and 'GPE' in n.label():
                phrase = ''
                for l in n.leaves():
                    phrase += l[0] + ' '
                self.candidates.append(phrase)
    
    
    def candidates_with_regexp(self):
        grammar = r'NP: {<JJ>*<NN.*>+}' # From NLTK book pp. 266.
        parser = nltk.RegexpParser(grammar)
        parsed = parser.parse(self.tagged_words)
        self.candidates = []
        for n in parsed:
            if isinstance(n, nltk.tree.Tree) and 'NP' in n.label():
                phrase = ''
                for l in n.leaves():
                    phrase += l[0] + ' '
                self.candidates.append(phrase)


    def match_countries(self, all_countries):
        self.countries = []
        for row_index, row in all_countries.iterrows():
            country_name = row.loc['Country']
            country_name_re = r'\b' + country_name + r'\b'
            for candidate in self.candidates:
                x = re.search(country_name_re, candidate)
                if x:
                    self.countries.append(row.loc['ISO'])
    
    
    def match_places(self, all_places):
        self.places = []
        keep = all_places['countrycode'].map(lambda x: x in self.countries)
        for row_index, row in all_places[keep].iterrows():
            place_name = row.loc['name']
            place_name_re = r'\b' + place_name + r'\b'
            for candidate in self.candidates:
                x = re.search(place_name_re, candidate)
                if x:
                    self.places.append(row)
        self.places = DataFrame(self.places)


# We also have to update the match_places_in_article function.

def match_places_in_article(paths_queue, matches_queue):
    for args in iter(paths_queue.get, 'STOP'):
        path, places, countries, tag = args
        x = article_nltk(path)
        x.get_tag_text(tag)
        x.pos_tag_words()
        x.candidates_with_regexp()
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