import datetime

from pubs import *

t1 = datetime.datetime.now()
articlepaths, places, countries = build_test_paths()
tag = 'p'
allmatches = get_all_matches_starmap(articlepaths, places, countries, tag)
t2 = datetime.datetime.now()
t2 - t1


# partial_get_single_article_matches = partial(get_single_article_matches,
#                                              places=places,
#                                              countries=countries,
#                                              tag=tag)

# pool = Pool(4)
# matches_list = pool.map(partial_get_single_article_matches, articlepaths)