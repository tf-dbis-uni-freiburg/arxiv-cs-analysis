""" This module goes through all the noun phrase files (90278), counts the total no. of phrases in each file, and inserts these
into a dataframe, along with the published date from the arxiv metadata Solr collection."""

import os
from glob import iglob
import pandas as pd
import requests
import pickle

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

def count_phrase_urls():
    """Creates a data frame from all the noun phrase wiki files. The data frame contains
    the filename, no. of phrase urls in each file and the published date from the arxiv
    xml file. This dataframe is finally pickled."""
    df = pd.DataFrame(columns=['filename', 'published_date', 'num_phrase_urls'])
    basepath = '/home/ashwath/Files/arxiv-cs-dataset-LREC2018-xlisa-annotations'
    # Initialize to -1 as we want to insert from loc[0] into the dataframe.
    file_num = -1
    for filepath in iglob(os.path.join(basepath, '*annotations.txt')):
        file_num += 1
        with open(filepath, 'r') as file:
            # Get the filename without extension (only 1st part before underscore)
            filename= os.path.basename(filepath)
            filename = filename.split('_')[0]
            published_date = search_solr(filename, 'arxiv_metadata', 'arxiv_identifier')
            # Get the line count: this will give the total no. of noun phrases (there is one noun phrase in each line)
            # All the lines are normalized, empty lines have already been removed using sed in pre-processing.  
            for line_num, line in enumerate(file):
                pass 
        num_lines = line_num + 1
        df.loc[file_num] = {'filename':filename, 'published_date':published_date, 'num_phrase_urls': num_lines}
        
    # Pickle the dataframe
    pickle_temp = open("total_phrase_wiki_counter.pickle", "wb")
    pickle.dump(df, pickle_temp)
    pickle_temp.close()

if __name__ == '__main__':
    count_phrase_urls()
