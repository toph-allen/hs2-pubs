# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

# import numpy as np
# from pandas import Series, DataFrame
# import pandas as pd
# from bs4 import BeautifulSoup
import datetime

# <codecell>

import pubs
reload(pubs)
from pubs import *

# <codecell>

articlepaths = get_article_paths(testdir)
places = get_geoname_dataframe(cities1000)
countries = get_countryinfo_dataframe(countryInfo)

# <codecell>

t1 = datetime.datetime.now()
affcounts = count_all_matches(articlepaths, places, countries, 'aff')
affcounts = affcounts.join(places)

# <codecell>

affcounts.to_csv(outdir + 'matched_places_aff.csv', encoding = 'utf-8')
t2 = datetime.datetime.now()
tdaff = t2 - t1
tdaff

# <codecell>

t1 = datetime.datetime.now()
pcounts = count_all_matches(articlepaths, places, countries, 'p')
pcounts = pcounts.join(places)

# <codecell>

pcounts.to_csv(outdir + 'matched_places_p.csv', encoding = 'utf-8')
t2 = datetime.datetime.now()
tdp = t2 - t1
tdp

