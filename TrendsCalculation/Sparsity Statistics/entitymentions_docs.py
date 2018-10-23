import requests
import sys
import pandas as pd
import json
import pickle


def search_solr_parse_json():
    """ """
    solr_url = 'http://localhost:8983/solr/nounphrases_wikipedia/select'
    # Exact search only
    url_params = {'q': '*', 'rows': 8000000, 'fl': 'wikipedia_url,num_occurrences'}
    solr_response = requests.get(solr_url, params=url_params)
    if solr_response.ok:
        data = solr_response.json()
        docs_df = pd.DataFrame(data['response']['docs'])
        return docs_df
    else:
        print("Invalid response returned from Solr")
        sys.exit(11)


if __name__ == '__main__':
    docs_df = search_solr_parse_json()
    # Group by wikipedia url
    docs_df = docs_df.groupby('wikipedia_url').count().rename(columns={'num_occurrences': 'num_documents'})
    print(docs_df.head())
    docs_df = docs_df.sort_values(by='num_documents', ascending=False)
    docs_df.to_csv('entity_mentions_docs_descending.tsv', sep='\t')

