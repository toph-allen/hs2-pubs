import datetime
from pubs import *

articlepaths = get_article_paths(articledir)
places = get_geoname_dataframe(cities1000)
countries = get_countryinfo_dataframe(countryInfo)

print 'Matching places in \'aff\' tag...'

t1 = datetime.datetime.now()
affcounts = count_all_matches(articlepaths, places, countries, 'aff')
affcounts = affcounts.join(places)

affcounts.to_csv(outdir + 'all_matched_places_aff.csv', encoding = 'utf-8')
t2 = datetime.datetime.now()
tdaff = t2 - t1
tdaff

print 'Matching places in \'p\' tag...'

t3 = datetime.datetime.now()
pcounts = count_all_matches(articlepaths, places, countries, 'p')
pcounts = pcounts.join(places)

pcounts.to_csv(outdir + 'all_matched_places_p.csv', encoding = 'utf-8')
t4 = datetime.datetime.now()
tdp = t4 - t3
tdp