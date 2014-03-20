import os
import csv
from time import sleep
import random

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

    def write_html_to_file(self):
        filename = (str(self.number) + ' ' + self.title.lower()).replace(' ', '_')[0:64] + '.html'
        outdir = os.getcwd() + '/laura_out/'
        file = open(outdir + filename, 'w')
        file.write(self.html)
        file.close()


class pub_html_getter(webdriver.Firefox):
    def __init__(self):
        self.ezproxy_username = 'aja2149'
        self.ezproxy_password = 'Ameliajean2!'
        super().__init__()

    def search_scholar_for_article(self, article):
        self.get('http://scholar.google.com')
        self.find_element_by_id('gs_hp_tsi').send_keys(article.search_string, Keys.RETURN)

    def click_first_result(self):
        self.find_element_by_class_name("gs_rt").find_element_by_tag_name('a').click()

    def ezproxy_login(self):
        ezproxy_url = 'http://ezproxy.cul.columbia.edu/login?url='
        target_url = self.current_url
        self.get(ezproxy_url + self.current_url)
        try:
            self.find_element_by_name('username').send_keys(self.ezproxy_username)
            self.find_element_by_name('password').send_keys(self.ezproxy_password, Keys.RETURN)
        except NoSuchElementException:
            pass

    def go_to_full_article(self):
        try:
            return self.find_element_by_partial_link_text('HTML').click()
        except NoSuchElementException:
            pass
        try:
            return self.find_element_by_partial_link_text('Full Article').click()
        except NoSuchElementException:
            pass
        try:
            return self.find_element_by_partial_link_text('Full').click()
        except NoSuchElementException:
            pass
        try:
            return self.find_element_by_partial_link_text('Article').click()
        except NoSuchElementException:
            pass
        raise NoSuchElementException

    def article_link_search_test(self):
        try:
            return self.find_element_by_partial_link_text('HTML').text
        except NoSuchElementException:
            pass
        try:
            return self.find_element_by_partial_link_text('Full Article').text
        except NoSuchElementException:
            pass
        try:
            return self.find_element_by_partial_link_text('Full').text
        except NoSuchElementException:
            pass
        raise NoSuchElementException
        try:
            return self.find_element_by_partial_link_text('Article').text
        except NoSuchElementException:
            pass

    def assign_article_current_page_source(self, article):
        article.html = self.page_source


def main():
    pub_dataframe = pd.io.parsers.read_table('data/laura_pubs_meta.csv', encoding='utf-8', quotechar='"', sep=',', index_col=0)
    pub_dataframe['URL'] = ''

    pub_dataframe = pub_dataframe.iloc[0:10, :]

    getter = pub_html_getter()

    for i, row in pub_dataframe.iterrows():
        article = pub_data(row)
        article.get_html_for_article(getter)
        article.write_html_to_file()
        pub_dataframe.loc[i, "URL"] = getter.current_url
        sleep(random.lognormvariate(1.5,0.5))

    pub_dataframe.to_csv(os.getcwd() + '/laura_out/laura_pub_urls.csv')


if __name__ == '__main__':
    main()
