import nltk
from pubs_queue import *

articlepaths = get_article_paths(testdir)
places = get_geoname_dataframe(cities1000)
countries = get_countryinfo_dataframe(countryInfo)

x = article(articlepaths[1])
x.get_tag_text('p')
x.match_countries(countries)
x.match_places(places)

tokens = nltk.word_tokenize(x.text)
tagged = nltk.pos_tag(tokens)

placelist = []

for (word, tag) in tagged:
    if tag.startswith('NN') and word in places['name'].tolist():
        placelist.append(word)

# Notes:
# 1. Use bigrams() and trigrams() functions from nltk
# 2. We'll still do the country matching thing just because it's a good idea.