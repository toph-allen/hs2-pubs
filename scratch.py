import datetime

from pubs import *

articlepaths, places, countries = build_test_paths()
tag = 'p'

t1 = datetime.datetime.now()

for i, chunk in enumerate(grouper(articlepaths, 40)):
    allmatches = get_all_matches_starmap(articlepaths, places, countries, tag)
    allmatches.to_csv(outdir + 'parallel_scratch{:}.csv'.format(i), encoding='utf-8')
    del allmatches

t2 = datetime.datetime.now()
print(t2 - t1)



