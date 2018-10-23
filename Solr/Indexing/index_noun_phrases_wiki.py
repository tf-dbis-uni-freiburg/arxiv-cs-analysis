""" This module is used to index the per-file frequency of every noun phrase in the 90278 noun phrase input files."""

import os
from collections import Counter
from glob import iglob
import pysolr
import requests

def search_solr(query, collection, search_field):
    """ Searches the arxiv_metadata collection on arxiv_identifier (query)
    and returns the published date which it obtains from parse_arxiv_json"""
    solr_url = 'http://localhost:8983/solr/' + collection + '/select'
    # Exact search only
    query = '"' + query + '"'
    url_params = {'q': query, 'rows': 1, 'df': search_field}
    solr_response = requests.get(solr_url, params=url_params)
    if solr_response.ok:
        data = solr_response.json()
        return parse_arxiv_json(data)
    else:
        print("Invalid response returned from Solr")
        sys.exit(11)

def parse_arxiv_json(data):
    """ Parses the response from the arxiv_metadata collection in Solr 
    for the exact search on arxiv_identifier. It returns the published date of 
    the first version of the paper (i.e. if there are multiple versions, it 
    ignores the revision dates)"""
    docs = data['response']['docs']
    # There is only one result returned (num_rows=1 and that is the nature of the
    # data, there is only one paper with 1 arxiv identifier). As a JSON array is 
    # returned and we only want the first date, we take only the first element of the
    # array.
    return docs[0].get('published_date')[0]    

def insert_into_solr():
    """ Inserts records into an empty solr index which has already been created. It inserts
    frequencies of each noun phrase per file along with the arxiv identifier (from the file
    name) and the published date (obtained from the arxiv_metadata Solr index)."""
    solr = pysolr.Solr('http://localhost:8983/solr/nounphrases_wikipedia')
    folderpath = '/home/ashwath/Files/arxiv-cs-dataset-LREC2018-xlisa-annotations'
    # Create an empty counter and update counts for phrases in each file inside the for loop.
    phrase_url_counter = Counter()
    for filepath in iglob(os.path.join(folderpath, '*annotations.txt')):
        # Insert all the phrases in a file into Solr in a list
        list_for_solr = []
        with open(filepath, "r") as file:
            # Get the filename without extension (only 1st part before underscore)
            filename= os.path.basename(filepath)
            filename = filename.split('_')[0]
            # published date is a default dict with lists as values.
            published_date = search_solr(filename, 'arxiv_metadata', 'arxiv_identifier')
            # Line is tab-separated (phrase_url, phrase,, start, end). We want only phrase_url
            # Don't add useless phrase urls to list 'phrases'. Use a generator
            # expression instead of a list comprehension
            phrase_urls = (line.split("\t")[0].lower().strip() for line in file 
                if line.split("\t")[0].lower().strip() != "")
            temp_phrase_url_counter = Counter(phrase_urls)
            for phrase_url, frequency in temp_phrase_url_counter.items():
                solr_content = {}
                solr_content['wikipedia_url'] = phrase_url
                solr_content['num_occurrences'] = frequency
                solr_content['published_date'] = published_date
                solr_content['arxiv_identifier'] = filename
                list_for_solr.append(solr_content)
        # Upload to Solr file by file
        solr.add(list_for_solr)

if __name__ == '__main__':
    insert_into_solr()