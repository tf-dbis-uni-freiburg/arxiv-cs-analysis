import requests
import json
import os
import argparse
import pandas as pd
import pickle

def search_solr_parse_json(query, collection, search_field):
    """ Searches the nounphrases collection on the published 'published_date' 
    field parses the json result and returns it as a list of dictionaries where
    each dictionary corresponds to a record. 
    ARGUMENTS: query, string: a query made up of a from and a to date, separated
                              by the word 'TO' as per Solr syntax
               collection: the Solr collection name (=nounphrases)
               search_field: the Solr field which is queried (=phrase)
    RETURNS: docs, list of dicts: the documents (records) returned by Solr 
             AFTER getting the JSON response and parsing it."""
    solr_url = 'http://localhost:8983/solr/' + collection + '/select'
    # Exact search only
    query = '"' + query + '"'
    # for rows, pass an arbitrarily large number.
    url_params = {'q': query, 'rows': 10000000, 'df': search_field}
    solr_response = requests.get(solr_url, params=url_params)
    if solr_response.ok:
        data = solr_response.json()
        docs = data['response']['docs']
        return docs
    else:
        print("Invalid response returned from Solr")
        sys.exit(11)

def dataframe_from_solr_results(documents_list):
    """ Takes a list of dictionaries (each dictionary is a record) obtained by parsing
    the JSON results from Solr, converts it into a dataframe, and keeps only the
    important columns (discards _version_, id, published_date and arxiv_identifier, keeps
    phrase, num_occurrences). Finally, it makes sure that the phrase
    is the new index.
    ARGUMENTS: documents_list, list of dicts: list of documents (records) returned
               by Solr for one search query
    RETURNS: docs_df, Pandas dataframe, the Solr results converted into a Pandas
             dataframe with index=published_date, columns=arxiv_identifier, num_occurrences"""
    docs_df = pd.DataFrame(documents_list)
    # Remove any rows which have phrase starting with #
    docs_df.drop(docs_df[docs_df['phrase'].str.startswith('#')].index, inplace=True)
    # Also, drop all phrases which do not contain a character (phrases composed of numbers and
    # special characters only. E.g. 25%, 13, 8&26 will all be dropped)
    pattern = r'[a-z]'
    # Use tilde for anything which doesn't match the pattern.
    docs_df.drop(docs_df[~docs_df['phrase'].str.contains(pattern)].index, inplace=True)
    docs_df.drop(['_version_', 'id', 'published_date', 'arxiv_identifier'], axis=1, inplace=True)
    # Make sure the phrase is the index. Once it is the index, we don't
    # really need the column any more.
    docs_df.set_index('phrase', inplace=True, drop=True)
    return docs_df

def group_by_phrase(docs_df):
    """ Takes a Pandas data frame with index=phrase, cols: num_occurrences as input,
    calculates the no. of unique and total occurrences of each phrase over
    the whole time period (a quarter) by grouping by phrase and aggregating 
    on the column num_occurrences (sum and count). The aggregated results are suitably 
    renamed, and the resulting data frame is returned.
    ARGUMENTS: docs_df, Pandas dataframe with index=phrase,
               columns=num_occurrences
    RETURNS: docs_df_grouped, a Pandas df grouped on phrase, on
             which 'sum' and 'count' are applied on num_occurrences.
    """
    # Dataframe takes the sum and count of num_occurrences after grouping by phrase (index)
    docs_df_grouped = docs_df.groupby(docs_df.index).num_occurrences.agg(['sum','count']).rename(
        columns={'sum':'total_occurrences','count':'total_docs'})

    # Change to int after replacing nan by 0
    docs_df_grouped.total_occurrences = docs_df_grouped.total_occurrences.fillna(0).astype('int64')
    docs_df_grouped.total_docs = docs_df_grouped.total_docs.fillna(0).astype('int64')
    return docs_df_grouped

def remove_useless_phrases(docs_df):
    """ Remove phrases starting with a special character, and those which don't have a letter in them"""
    # Remove any rows which have phrase starting with special characters
    specialcharacters = ('|', '#', '*', '%', '@', '!', '~', '&', '>', '<', '\\', '/', '?', ';',
                         ':', ']', '[', '}', '{', '(', ')', '_', '-', '=', '+', '^')
    docs_df.drop(docs_df[docs_df.index.str.startswith(specialcharacters)].index, inplace=True)
    # Also, drop all phrases which do not contain a character (phrases composed of numbers and
    # special characters only. E.g. 25%, 13, 8&26 will all be dropped)
    pattern = r'[a-z]'
    # Use tilde for anything which doesn't match the pattern.
    docs_df.drop(docs_df[~docs_df.index.str.contains(pattern)].index, inplace=True)
    return docs_df

def normalize_dataframe(df):
    """ Converts the total occurrences and total docs into percentages"""
    df.total_occurrences = df.total_occurrences * 100 / df.total_occurrences.sum()
    df.total_docs = df.total_docs * 100 / df.total_docs.sum()
    return df

def make_date_range_query(from_date, to_date):
    """ Forms a Solr date range query of form [from_date TO to_date] based on the
    values of from_date and to_date in the arguments."""
    query = "[{} TO {}]".format(from_date, to_date)
    return query

def grouped_dataframes_from_daterange(from_date, to_date):
    """ Takes a from date and a to date, queries the Solr index nounphraes on the published_date field
    using these dates, converts the result into a Pandas dataframe, groups the data frame by phrase and
    calculates sum and count (total and unique occurrences: unique occurrences = no. of docs). Returns
    the resulting dataframe"""
    docs = search_solr_parse_json(make_date_range_query(from_date, to_date),
                                  'nounphrases', 'published_date')
    docs_df = dataframe_from_solr_results(docs)
    grouped_docs_df = group_by_phrase(docs_df)
    grouped_docs_df = remove_useless_phrases(grouped_docs_df)
    normalized_df = normalize_dataframe(grouped_docs_df)
    normalized_df.sort_values(by='total_occurrences', ascending=False, inplace=True)
    return normalized_df

def write_to_file(df, filename, subfolder):
    """ Writes a data frame into a file specified by 'filename' in the subfolder specified by 'subfolder'."""
    if not os.path.exists(subfolder):
        os.makedirs(subfolder)
    df.to_csv('{}/{}'.format(subfolder, filename), sep='\t')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("fromdate", help='Specify the from date (yyyy-mm-dd)')
    parser.add_argument("todate", help='Specify the to date (yyyy-mm-dd)')
    parser.add_argument("filename", help='Specify the file name to write the results of the query to')
    args=parser.parse_args()
    df = grouped_dataframes_from_daterange(args.fromdate, args.todate)
    write_to_file(df, args.filename, "QueryResults/")   

if __name__ == '__main__':
    main()