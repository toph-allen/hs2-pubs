# Objects

I'm thinking we have a few objects that can talk to each other, i.e. call methods on one another.

- The laura_pubs data frame
- For each article in that data frame, we create an object representing that article with metadata in attributes like article.title and article.journal.vol
    - A method to write out its xml to a file in a directory.
- A selenium object that is basically a firefox browser. It has additional methods:
    - to take an article and get the title of that article and create a search string and search Google Scholar for it
        - `browser.create_search_string`
        - `browser.google_scholar_lucky`
        - `browser.view_through_eproxy`
        - etc.
    - then it can give the article object the xml string or something, assign it to article.xml