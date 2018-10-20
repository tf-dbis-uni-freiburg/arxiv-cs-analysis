""" Module to create files with positively trending and negative trending noun phrases between Sept-Dec 2007
and Sept-Dec 2017"""
import requests
import json
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

def get_counts_from_json(months_start, months_end):
    """ Reads a json file containing counts for each month and calculates no. of total phrases and no.
    of total documents in the required periods based on the arguments months_start and months_end, 
    which are lists of months in which the calculation has to be done. These 2 lists contain keys
    in the 2 dicts of the JSON file and can be used directly to access the values""" 
    # Read the Json file which has the monthly total phrases and documents -- 2 Json objects in a 
    # json array. Assign each object to a dictionary.
    with open('phrases_and_docs_monthly.json', 'r') as file:
        json_array= json.load(file)
    # json_array is a list of 2 dicts.
    monthly_phrases_total = json_array[0]
    monthly_docs_total = json_array[1]
    start_months_phrases_sum = sum([monthly_phrases_total.get(month) for month in months_start])
    start_months_docs_sum = sum([monthly_docs_total.get(month) for month in months_start])
    end_months_phrases_sum = sum([monthly_phrases_total.get(month) for month in months_end])
    end_months_docs_sum = sum([monthly_docs_total.get(month) for month in months_end])
    return start_months_phrases_sum, start_months_docs_sum, end_months_phrases_sum, end_months_docs_sum

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
    return grouped_docs_df

def normalize_dataframes(start_df, end_df):
    """ Takes the dataframes pertaining to the ranges Sept-Dec 2007 and Sept-Dec 2017, and normalizes the
    total_occurrences and total_docs columns by dividing them by the total no. of phrases or total no. of
    docs in the respective period and multiplying by 100 to create percentages.
    The columns in the output data frames are renamed to percentage_occurrences and percentage_docs
    """
    months_start = ['2007-09', '2007-10', '2007-11', '2007-12']
    months_end = ['2017-09', '2017-10', '2017-11', '2017-12']
    # Get the 2 counts for the 2 quarters (2007 last quarter, 2017 last quarter)
    last_qtr_2007_phrases, last_qtr_2007_docs, last_qtr_2017_phrases, last_qtr_2017_docs = get_counts_from_json(
        months_start, months_end)
    start_df['percentage_occurrences'] = start_df['total_occurrences'] * 100 / last_qtr_2007_phrases
    start_df['percentage_docs'] = start_df['total_docs'] * 100 / last_qtr_2007_docs
    end_df['percentage_occurrences'] = end_df['total_occurrences'] * 100 / last_qtr_2017_phrases
    end_df['percentage_docs'] = end_df['total_docs'] * 100 / last_qtr_2017_docs

    # Drop the total occurrences and total docs columns, they are no longer necessary
    start_df.drop(['total_occurrences', 'total_docs'], axis=1, inplace=True)
    end_df.drop(['total_occurrences', 'total_docs'], axis=1, inplace=True)
    return start_df, end_df

def join_dfs_on_phrase(start_df, end_df):
    """ Takes dataframes pertaining to the 4 'start' months and the 4 'end' months, both of which have
    already been grouped, and joins them on the index (phrase) using an inner join. It also creates 2 new
    columns by subtracting the  total and unique phrases in the 'start' from the corresponding values in the 'end'
    start_df and end_df have the index 'phrase' and the columns 'percentage_occurrences' and 'percentage_docs'
    The columns 'phrase_diffeeneces' and 'doc_differences' are added to the output."""
    joined_df = pd.merge(start_df, end_df, how='inner', left_index=True, right_index=True, suffixes=['_start', '_end'])
    joined_df['phrase_differences'] = joined_df['percentage_occurrences_end'] - joined_df['percentage_occurrences_start']
    joined_df['doc_differences'] = joined_df['percentage_docs_end'] - joined_df['percentage_docs_start']
    return joined_df

def create_sorted_stats_dfs(joined_df):
    """ Takes a dataframe with phrase as index and percentage_occurrence_start, percentage_docs_start,
    percentage_occurrences_end, percentage_docs_end (start and end are two different time periods: 4th quarter
    in 2007 and 4th quarter in 2017 resp.), phrase_differences, doc_differences (end-start for phrase count and
    doc count for each phrase) as columns, and produces 4 dataframes: (i) phrases with positive phrase_differences
    (in descending order), (ii) phrases with negative phrase_differences (in ascending order), (iii) phrases with
    postive doc_differences (in descending order), (iv) phrases with negative doc_differences (in ascending order).
    Note that phrase_differences and doc_differences are expressed in percentages, and not whole numbers. Also, note
    that phrases which don't show a trend (0 difference) are not inserted into any of the 4 dataframes."""
    positive_phrases_diff = joined_df[joined_df.phrase_differences>0]
    positive_phrases_diff = positive_phrases_diff.sort_values(by='phrase_differences', ascending=False)
    negative_phrases_diff = joined_df[joined_df.phrase_differences<0]
    # Ascending order for negative: we want high negative values to be first.
    negative_phrases_diff = negative_phrases_diff.sort_values(by='phrase_differences')
    positive_docs_diff = joined_df[joined_df.doc_differences>0]
    positive_docs_diff = positive_docs_diff.sort_values(by='doc_differences', ascending=False)
    negative_docs_diff = joined_df[joined_df.doc_differences<0]
    # Ascending order for negative: we want high negative values to be first.
    negative_docs_diff = negative_docs_diff.sort_values(by='doc_differences')
    print(positive_phrases_diff.head(), positive_docs_diff.head(), negative_phrases_diff.head(), negative_docs_diff.head())
    print(positive_phrases_diff.shape, positive_docs_diff.shape, negative_phrases_diff.shape, negative_docs_diff.shape)
    return positive_phrases_diff, negative_phrases_diff, positive_docs_diff, negative_docs_diff

def write_to_files(positive_phrases_diff, negative_phrases_diff, positive_docs_diff, negative_docs_diff):
    """ Writes the 4 dataframes in the arguments to four csv files."""
    positive_phrases_diff.to_csv('Output/SeptDec2007_2017/positive_phrases_differences.tsv', sep='\t')
    negative_phrases_diff.to_csv('Output/SeptDec2007_2017/negative_phrases_differences.tsv', sep='\t')
    positive_docs_diff.to_csv('Output/SeptDec2007_2017/positive_docs_differences.tsv', sep='\t')
    negative_docs_diff.to_csv('Output/SeptDec2007_2017/negative_docs_differences.tsv', sep='\t')

def main():
    """Main function which gets the frequencies of phrases in 2 quarters (in dataframes), joins them and
    calculates statistics from them."""
    try:
        # If pickle already exists, there is no need to make the data frames again.
        pickle_start = open('Pickles/SeptDec_2007_df.pickle', 'rb')
        start_df = pickle.load(pickle_start)
        pickle_end = open('Pickles/SeptDec_2017_df.pickle', 'rb')
        end_df = pickle.load(pickle_end)

    except FileNotFoundError:
        # Pickle doesn't exist, we need to go through the whole process
        # Get dataframes based on 2 Solr date range queries
        start_df = grouped_dataframes_from_daterange('2007-09-01', '2007-12-31')
        end_df = grouped_dataframes_from_daterange('2017-09-01', '2017-12-31')
        start_pickle = open('Pickles/SeptDec_2007_df.pickle', 'wb')
        pickle.dump(start_df, start_pickle)
        start_pickle.close()
        end_pickle = open('Pickles/SeptDec_2017_df.pickle', 'wb')
        pickle.dump(end_df, end_pickle)
        end_pickle.close()
    # Normalize by dividing the total_occurrences and total_docs by the total no. of phrases and total no. of docs
    # respectively, and multiplying by 100 to convert to a percentage
    # Join on the index (phrase) -- inner join
    start_df, end_df = normalize_dataframes(start_df, end_df)
    joined_df = join_dfs_on_phrase(start_df, end_df)
    positive_phrases_diff, negative_phrases_diff, positive_docs_diff, negative_docs_diff = create_sorted_stats_dfs(joined_df)
    # Write the dataframes to 4 ouptut files
    write_to_files(positive_phrases_diff, negative_phrases_diff, positive_docs_diff, negative_docs_diff)

if __name__ == '__main__':
    main()