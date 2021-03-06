# -*- coding: utf-8 -*-
"""
    #-------------------------------------------------------------------------------
    # Name:        CREATE PAPERS PLUS
    # Purpose:     Uses indices for papers, metadata and arxiv-metadata
    #              and inserts data into a new index 'papers_plus' which adds
    #              data from the other indices to the references index
    #
    # Author:      Ashwath Sampath
    #
    # Created:     21-10-2018
    # Revised:     8-12-2018
    # Copyright:   (c) Ashwath Sampath 2018
    #-------------------------------------------------------------------------------

"""
import os
import csv
from collections import defaultdict
import requests
import datetime
import pysolr
from glob import glob
from time import time
import concurrent.futures

# Make a connection to Solr
solr = pysolr.Solr('http://localhost:8983/solr/papers_plus')

def search_solr(query, collection, search_field, num_rows):
    """ Searches the specified collection on the specified search_field (and a
    specified no. of rows) and fetches and retuens results using parse_json"""
    solr_url = 'http://localhost:8983/solr/' + collection + '/select'
    # Exact search only
    query = '"' + query + '"'
    url_params = {'q': query, 'rows': num_rows, 'df': search_field}
    solr_response = requests.get(solr_url, params=url_params)
    if solr_response.ok:
        data = solr_response.json()
        return parse_json(data, collection)
    else:
        print("Invalid response returned from Solr")
        sys.exit(11)


def parse_json(data, collection):
    """ Calls the appropriate json parser based on the collection,
    returns whatever the parser returns, along with the query and
    num_responses, which it gets from the json response. If there
    are no results, it returns ([], query, 0)"""
    # FIRST (only applies to references_plus index), check if this
    # annotation is already present
    if collection == 'references_plus':
        return parse_references_plus_json(data)
    # query is the actual phrase searched in Solr
    query = data['responseHeader']['params']['q']
    num_responses = data['response']['numFound']
    if num_responses == 0:
        return []
    elif collection == 'arxiv_metadata':
        results = parse_arxiv_metadata_json(data)
    elif collection == 'metadata':
        results = parse_metadata_json(data)
    return results

def parse_arxiv_metadata_json(data):
    """ Function to parse the json response from the metadata or the
    arxiv_metadata collections in Solr. It returns the results as a
    list with the sentence, file name and title."""
    # docs contains authors, title, id generated by Solr, url
    docs = data['response']['docs']
    # NOTE: there are records without authors and urls. This is why the
    # get method is always used to get the value instead of getting the value
    # by applying the [] operator on the key.
    results = [[docs[i].get('title'), docs[i].get('authors'),
                docs[i].get('url'), docs[i].get('published_date')]
                for i in range(len(docs))]
    return results
    
def parse_metadata_json(data):
    """ Function to parse the json response from the metadata or the
    arxiv_metadata collections in Solr. It returns the dblp url. 
    docs is a list of one result, so this is obtained by docs[0].get('url')"""
    # docs contains authors, title, id generated by Solr, url
    docs = data['response']['docs']
    # NOTE: there are records without authors and urls. This is why the
    # get method is always used to get the value instead of getting the value
    # by applying the [] operator on the key.

    dblp_url = docs[0].get('url') 
    return dblp_url

def parse_file_build_index(filepath):
    """ Read each of the txt files, which have sentences (with annotations). Use the file name (arxiv
    identifier) to get metadata from the arxiv_metadata and the metadata indices. Insert all the fields
    in a new index papers_plus.
    Solr field definition for new Solr index papers_plus:

    <!-- Papers -->
    <field name="sentence" type="text_classic" indexed="true" stored="true" multiValued="false"/>
    <field name="arxiv_identifier" type="string" indexed="true" stored="true" multiValued="false"/>
    
    <!-- arxiv metadata-->
    <field name="arxiv_url" type="string" indexed="true" stored="true" multiValued="false"/> 
    <field name="authors" type="text_classic" indexed="true" stored="true" multiValued="false"/> 
    <field name="title" type="text_classic" indexed="true" stored="true" multiValued="false"/> 
    <field name="published_date" type="daterange" indexed="true" stored="true" multiValued="false"/>
    <field name="revision_dates" type="string" indexed="true" stored="true" multiValued="false"/>

    <!-- meta field: dblp_url-->
    <field name="dblp_url" type="string" indexed="true" stored="true" multiValued="false"/> 


     """

    with open(filepath, 'r') as file:
        list_for_solr = []
        filename = os.path.basename(filepath)
        print(filename)
        arxiv_identifier = '.'.join(filename.split('.')[:2])
        linenum = 0
        for line in file:
            # Many lines have just ======, do not index them
            if not line.startswith('=='):
                solr_record = {}
                linenum += 1
                solr_record['sentence'] = line.replace('\n', '')
                solr_record['sentencenum'] = linenum
                # arxiv identifier is also the primary key.
                solr_record['arxiv_identifier'] = arxiv_identifier
                # Primary key is arxiv_identifier concatenated with the sentence number.
                solr_record['id'] = "{}.{}".format(arxiv_identifier, linenum) 
                # Read the metadata from the arxiv_metadata index with a dot in between
                arxiv_metadata_result = search_solr(arxiv_identifier, 'arxiv_metadata', 'arxiv_identifier', 1)
               
                # Get the dblp url from the metadata index
                dblp_url = search_solr(arxiv_identifier, 'metadata', 'arxiv_identifier', 1)
                dblp_url = dblp_url if dblp_url is not None else 'unavailable'
                solr_record['dblp_url'] = dblp_url
                for title, authors, arxiv_url, published_dates in arxiv_metadata_result:
                    # Flatten the published dates into a single string. Not using a DateRange field because
                    # a grouping is done in django_paper_search which needs the dates to be a single string
                    if len(published_dates) == 1:
                        solr_record['published_date'] = published_dates[0]
                        solr_record['revision_dates'] = 'unavailable'
                    else:
                        solr_record['published_date'] = published_dates[0]
                        revision = ';'.join([datetime.datetime.strptime(pdate[:10], '%Y-%m-%d').strftime('%B %d, %Y') for pdate in published_dates[1:]])
                        solr_record['revision_dates'] = 'revised on {}'.format(revision)
                        #print(published_dates)
                    solr_record['title'] = title
                    solr_record['authors'] = '; '.join(authors)
                    solr_record['arxiv_url'] = arxiv_url
                list_for_solr.append(solr_record)
        # Add to Solr after reading one file completely
        solr.add(list_for_solr)
        print("added")
        #print("Inserted list length =", len(list_for_solr))

def create_concurrent_futures():
    """ Uses all the cores to do the parsing and inserting"""
    folderpath = '/home/ashwath/Files/arxiv-cs-dataset-LREC2018/'
    text_files = glob(os.path.join(folderpath, '*.txt'))
    with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
        executor.map(parse_file_build_index, text_files)
                
if __name__ == '__main__':
    start_time = time()
    create_concurrent_futures()
    print("Completed in {} seconds!".format(time() - start_time))
    
