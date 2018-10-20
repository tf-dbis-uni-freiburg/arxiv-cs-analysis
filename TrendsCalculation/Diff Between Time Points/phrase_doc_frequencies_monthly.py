""" This module is used to create a data frame with all phrases which occur at lest 100 times in the whole corpus (the
common words which occur 50000 times or more are later discarded), gets the percentage of docs they occur in each
year from 2007 to 2017.
Note: it expects pickles produced by yearly_trends.py to be present"""

import os
import json
import requests
import copy
from collections import OrderedDict
import pandas as pd
pd.options.display.max_rows = 200


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
    # Remove any rows which have phrase starting with special characters
    specialcharacters = ('|', '#', '*', '%', '@', '!', '~', '&', '>', '<', '\\', '/', '?', ';',
                         ':', ']', '[', '}', '{', '(', ')', '_', '-', '=', '+', '^')
    docs_df.drop(docs_df[docs_df['phrase'].str.startswith(specialcharacters)].index, inplace=True)
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


def get_complete_df_from_pickles():
    """ Reads pickle which contain all the phrases in the corpus and the no. of docs in which they occur/their total
    frequency, removes phrases starting with special characters, and which don't contain a letter, and then joins
    the docs and total occurrences dataframes to produce one dataframe, which is returned."""
    total_phrase_occurrences = pd.read_pickle('Pickles/total_phrase_count_dataframe.pickle')
    total_phrase_occurrences = total_phrase_occurrences[total_phrase_occurrences['num_occurrences']>100]
    #total_phrase_occurrences.sort_values(by='num_occurrences', ascending=False).head(10)
    total_doc_occurrences = pd.read_pickle('Pickles/total_doc_count_dataframe.pickle')

    #total_doc_occurrences.sort_values(by='num_documents', ascending=False).head(200)
    total_phrase_occurrences.rename(columns={'num_occurrences': 'total_occurrences'}, inplace=True)
    total_doc_occurrences.rename(columns={'num_documents': 'total_documents'}, inplace=True)
    joined_df = pd.merge(total_phrase_occurrences, total_doc_occurrences, how='inner', left_index=True, right_index=True)
    joined_df = drop_useless_phrases(joined_df)
    return joined_df

def drop_useless_phrases(joined_df):
    """ Takes the joined dataframe which contains all phrases with their total occurrences in the corpus, total no. of
    docs in which they occur, and drops phrases which are common (>50000 total occurrences), or which start with a special
    character. Rare phrases (less than 100 total occurrences) have already been removed. Returns smaller dataframe. """
    # Drop all phrases which do not contain a character (phrases composed of numbers and
    # special characters only. E.g. 25%, 13, 8&26 will all be dropped)
    pattern = r'[a-z]'
    # Use tilde for anything which doesn't match the pattern.
    joined_df.drop(joined_df[~joined_df.index.str.contains(pattern)].index, inplace=True)
    # Drop phrases which start with a special character
    specialcharacters = ('|', '#', '*', '%', '@', '!', '~', '&', '>', '<', '\\', '/', '?', ';', ':', ']',
                         '[', '}', '{', '(', ')', '_', '-', '=', '+', '^')
    joined_df.drop(joined_df[joined_df.index.str.startswith(specialcharacters)].index, inplace=True)
    # Drop rows which have total_occurrences greater than 50000: common words
    joined_df.drop(joined_df[joined_df.total_occurrences>50000].index, inplace=True)
    return joined_df

def join_additional_dfs(joined_df, month_dataframe_dict):
    """ Joins each of the dataframes in the month_dataframe_dict (dict with keys=months, values=dfs) with joined_df, which
    has total_occurrences and total_docs of the phrases over the whole corpus. Some phrases have already been removed in
    a previous step, so this is a reduced data frame. The 11 years' percentage occurrences for all the phrases are present in
    year_dataframe_dict, and these are left joined to the joined datthe end (the % phrase frequency columns are removed) """
    # As dict is mutable, take a copy and work on that.
    other_years_dict = copy.deepcopy(month_dataframe_dict)
    for month, month_df in other_years_dict.items():
        percent_docs_with_suffix = 'percentage_docs_{}'.format(month)
        month_df.rename(columns={'percentage_docs': percent_docs_with_suffix}, inplace=True)
        # Left Join with existing joined_df (no need of suffix as there are no columns with the same name --- already renamed)
        joined_df = pd.merge(joined_df, month_df, how='left', left_index=True, right_index=True)
    joined_df.fillna(0, inplace=True)
    return joined_df

def get_docfreq_dict_from_json():
    """ Reads a json file containing total phrase and doc freqs for each month and returns a dictionary of total
    no. of documents in each of the months.""" 
    # Read the Json file which has the monthly total phrases and documents -- 2 Json objects in a 
    # json array. Assign each object to a dictionary.
    with open('phrases_and_docs_monthly.json', 'r') as file:
        json_array = json.load(file)
    # json_array is a list of 2 dicts (1st -> phrase freq, 2nd -> doc_freq).
    #monthly_phrases_total = json_array[0]
    monthly_docs_total = json_array[1]
    return monthly_docs_total

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

def grouped_dataframes_for_month(month, total_docs_dict):
    """ Takes a month as argument, queries the Solr index nounphraes on the published_date field using
    this month (it is a DateRange field in Solr) and gets the documents in JSON form. It also converts the 
    result into a Pandas dataframe, groups the data frame by phrase and calculates sum and count (total
    and unique occurrences: unique occurrences = no. of docs). It finally normalizes these 2 columns
    and returns the resulting dataframe"""
    docs = search_solr_parse_json(month, 'nounphrases', 'published_date')
    docs_df = dataframe_from_solr_results(docs)
    grouped_docs_df = group_by_phrase(docs_df)
    # Normalize by dividing the total_occurrences and total_docs by the total no. of phrases and total no.
    # of docs respectively, and multiplying by 100 to convert to a percentage.
    normalized_df = normalize_dataframes(grouped_docs_df, month, total_docs_dict)
    return normalized_df

def normalize_dataframes(grouped_df, month, total_docs_dict):
    """ Takes a dataframe which has been grouped by phrase and aggregated, and normalizes the
    total_occurrences and total_docs columns by dividing them by the total no. of phrases or total no. of
    docs in the relevant month and multiplying by 100 to create percentages.
    The columns in the data frame which is returned is renamed to percentage_occurrences and percentage_docs
    """
    # Get the no. of phrases and documents in the year in the arguments
    doc_count = total_docs_dict.get(month)
    grouped_df['percentage_docs'] = grouped_df['total_docs'] * 100 / doc_count
    # Drop the total occurrences and total docs columns, they are no longer necessary
    grouped_df.drop(['total_occurrences', 'total_docs'], axis=1, inplace=True)
    return grouped_df

def write_to_file(joined_df, subfolder):
    """ Writes the dataframe in the argument to a tsv file, which is inserted into a new subfolder
    (if it doesn't exist)."""
    path_to_insert = "Output/{}".format(subfolder)
    if not os.path.exists(path_to_insert):
        os.makedirs(path_to_insert)
    joined_df.to_csv('{}/monthly_phrases_docs_frequency.tsv'.format(path_to_insert), sep='\t')
    
def main():
    """ Main function which calls other functions to create a data frame from multiple pickles/read from an existing dataframe
    and to call functions to calculate the Mann Kendall and Theil sen statistics."""
    # Define a list with column names for the percentage docs for each year
    
    total_docs_dict = get_docfreq_dict_from_json()
    months = sorted(list(total_docs_dict.keys()))
    month_columns = ["percentage_docs_{}".format(month) for month in months]
    month_dataframe_dict = OrderedDict()
    for month in months:
        cur_month_df = grouped_dataframes_for_month(month, total_docs_dict)
        # Add the year and dataframe to year_dataframe_dict
        month_dataframe_dict[month] = cur_month_df
    # Get dataframe of all phrases
    total_df = get_complete_df_from_pickles()
    # Join to each of the monthly results
    joined_df = join_additional_dfs(total_df, month_dataframe_dict)
    # Sort the df by total documents in which they occur
    joined_df = joined_df.sort_values(by='total_documents', ascending=False)
    # Write to files
    subfolder = 'YearAndMonthPercentages'
    write_to_file(joined_df, subfolder)


if __name__ == '__main__':
    main()