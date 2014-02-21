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

# This script uses the old place matching code.

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