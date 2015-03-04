import os
import csv
import time
import random
import string

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
import numpy as np
import pandas as pd
from pandas import Series, DataFrame
from bs4 import BeautifulSoup


class pub_data:
    def __init__(self, row):
        self.number = row.loc['Number']
        self.journal = row.loc['Journal']
        self.volume = row.loc['Vol']
        self.year = row.loc['Year']
        self.issue = row.loc['Issue']
        self.first_page = row.loc['First.pg']
        self.title = row.loc['Title']
        self.search_string = '"' + self.title + '"'
        self.html = ''

    def get_html_for_article(self, getter):
        # This is the one that does the work
        getter.search_scholar_for_article(self)
        getter.click_first_result()
        getter.ezproxy_login()
        getter.go_to_full_article()
        getter.assign_article_current_page_source(self)

    def write_html_to_file(self, outdir):
        valid_chars = "-_.() {}{}".format(string.ascii_letters, string.digits)
        filename = (str(self.number) + ' ' + self.title.lower()).replace(' ', '_')[0:64] + '.html'
        filename = ''.join(c for c in filename if c in valid_chars)
        file = open(os.path.join(outdir, filename), 'w')
        file.write(self.html)
        file.close()


class pub_html_getter(webdriver.Firefox):
    def __init__(self):
        self.ezproxy_username = 'aja2149'
        self.ezproxy_password = 'Ameliajean2!'
        super().__init__()

    def search_scholar_for_article(self, article):
        self.get('http://scholar.google.com')
        try:
            self.find_element_by_id('gs_hp_tsi').send_keys(article.search_string, Keys.RETURN)
        except:
            pass

    def click_first_result(self):
        try:
            self.find_element_by_class_name("gs_rt").find_element_by_tag_name('a').click()
        except:
            pass

    def ezproxy_login(self):
        ezproxy_url = 'http://ezproxy.cul.columbia.edu/login?url='
        target_url = self.current_url
        self.get(ezproxy_url + self.current_url)
        try:
            self.find_element_by_name('username').send_keys(self.ezproxy_username)
            self.find_element_by_name('password').send_keys(self.ezproxy_password, Keys.RETURN)
        except:
            pass

    def go_to_full_article(self):
        try:
            return self.find_element_by_partial_link_text('HTML').click()
        except:
            pass
        try:
            return self.find_element_by_partial_link_text('Full Article').click()
        except:
            pass
        try:
            return self.find_element_by_partial_link_text('Full').click()
        except:
            pass
        try:
            return self.find_element_by_partial_link_text('Article').click()
        except:
            pass

    def article_link_search_test(self):
        try:
            return self.find_element_by_partial_link_text('HTML').text
        except:
            pass
        try:
            return self.find_element_by_partial_link_text('Full Article').text
        except:
            pass
        try:
            return self.find_element_by_partial_link_text('Full').text
        except:
            pass
        raise NoSuchElementException
        try:
            return self.find_element_by_partial_link_text('Article').text
        except:
            pass

    def assign_article_current_page_source(self, article):
        article.html = self.page_source


def main():
    start_time = time.strftime("%Y-%m-%d_%H-%M-%S")
    outdir = os.getcwd() + '/laura_out/' + start_time
    if not (os.path.exists(outdir)):
        os.makedirs(outdir)
    outcsv = open(os.path.join(outdir, 'laura_pub_urls.csv'), 'a')
    outcsv.write(',Number,Journal,Vol,Year,Issue,First.pg,Title,URL\n')
    outcsv.close()


    pub_dataframe = pd.io.parsers.read_table('data/laura_pubs_meta.csv', encoding='utf-8', quotechar='"', sep=',', index_col=0)
    pub_dataframe['URL'] = ''

    num_pubs = len(pub_dataframe)

    # Optionally operate on only a part of the list.
    # Percentage complete shows how far you are through the whole list.
    pub_dataframe = pub_dataframe.iloc[1090:, :]

    getter = pub_html_getter()

    for i, row in pub_dataframe.iterrows():
        print('Downloading article {} of {} '
              '({:.2%} complete).'.format(i, num_pubs, i / num_pubs))
        article = pub_data(row)
        article.get_html_for_article(getter)
        article.write_html_to_file(outdir)
        row.loc['URL'] = getter.current_url
        row = DataFrame(row).T
        with open(os.path.join(outdir, 'laura_pub_urls.csv'), 'a') as outcsv:
            row.to_csv(outcsv, header=False)
        time.sleep(random.lognormvariate(0.75,0.5))

    # outcsv.close()
    getter.close()


if __name__ == '__main__':
    main()
