import datetime
from pubs import *

articlepaths = get_article_paths('/Users/toph/PMC Articles/articles.I-N')
places = get_geoname_dataframe(cities1000)
countries = get_countryinfo_dataframe(countryInfo)

t1 = datetime.datetime.now()
affcounts = get_all_matches(articlepaths, places, countries, 'aff')

affcounts.to_csv(outdir + 'matched_places_aff i-n.csv', encoding = 'utf-8')
t2 = datetime.datetime.now()
tdaff = t2 - t1
tdaff

t1 = datetime.datetime.now()
pcounts = get_all_matches(articlepaths, places, countries, 'p')

pcounts.to_csv(outdir + 'matched_places_p i-n.csv', encoding = 'utf-8')
t2 = datetime.datetime.now()
tdp = t2 - t1
tdp

pcounts


