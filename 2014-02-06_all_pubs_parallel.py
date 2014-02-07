import datetime
from math import ceil

from pubs import *

articlepaths = get_article_paths('/Users/toph/PMC Articles/')
places = get_geoname_dataframe('cities1000.txt')
countries = get_countryinfo_dataframe('countryInfo.txt')
tag = 'p'

chunksize = 1000
chunks = chunker(articlepaths, chunksize)
numarticles = len(articlepaths)
numchunks = ceil(numarticles/chunksize)

class matchbox:
    def __init__(self, numarticles, chunksize):
        self.articles = 0
        self.chunks = 0
        self.nummatches = 0
        self.numarticles = len(articlepaths)
        self.numchunks = ceil(numarticles/chunksize)
        self.dataframe = DataFrame()

    def update(self, matches):
        self.articles += 1
        self.nummatches += len(matches)
        self.dataframe.append(matches, ignore_index=True)
        print('Matched {} places in article {} of {} ({:.2%} complete).'
              'Total matches: {}'.format(self.matches,
                                         self.articles,
                                         self.numarticles,
                                         self.articles / self.numarticles))

    def dump_into_csv(self):
        self.chunks += 1
        outname = outdir + articledir + '.chunk_' + self.chunks + '.csv'
        self.dataframe.to_csv(outname, encoding='utf-8')
        del self.dataframe
        self.dataframe = DataFrame()


allmatches = matchbox(numarticles, chunksize)

for chunk in chunks:

    pool = Pool(processes=8)
    args = zip(articlepaths, repeat(places), repeat(countries),
                  repeat(tag))
    m = pool.starmap_async(get_single_article_matches, args,
                           chunksize=10, callback=allmatches.update)
    m.wait()
    pool.close()
    del pool
    allmatches.dump_into_csv