""" Module to pickle yearly counts from 2007 to 2017 """

import os
import copy
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
    wikipedia_url, num_occurrences). Finally, it makes sure that the wikipedia_url
    is the new index.
    ARGUMENTS: documents_list, list of dicts: list of documents (records) returned
               by Solr for one search query
    RETURNS: docs_df, Pandas dataframe, the Solr results converted into a Pandas
             dataframe with index=published_date, columns=arxiv_identifier, num_occurrences"""
    docs_df = pd.DataFrame(documents_list)
    docs_df.drop(['_version_', 'id', 'published_date', 'arxiv_identifier'], axis=1, inplace=True)
    # Make sure the phrase is the index. Once it is the index, we don't
    # really need the column any more.
    docs_df.set_index('wikipedia_url', inplace=True, drop=True)
    return docs_df

def group_by_wikipedia_url(docs_df):
    """ Takes a Pandas data frame with index=wikipedia_url, cols: num_occurrences as input,
    calculates the no. of unique and total occurrences of each wikipedia_url over
    the whole time period (a quarter) by grouping by wikipedia_url and aggregating 
    on the column num_occurrences (sum and count). The aggregated results are suitably 
    renamed, and the resulting data frame is returned.
    ARGUMENTS: docs_df, Pandas dataframe with index=wikipedia_url,
               columns=num_occurrences
    RETURNS: docs_df_grouped, a Pandas df grouped on wikipedia_url, on
             which 'sum' and 'count' are applied on num_occurrences.
    """
    # Dataframe takes the sum and count of num_occurrences after grouping by wikipedia_url (index)
    docs_df_grouped = docs_df.groupby(docs_df.index).num_occurrences.agg(['sum','count']).rename(
        columns={'sum':'total_occurrences','count':'total_docs'})

    # Change to int after replacing nan by 0
    docs_df_grouped.total_occurrences = docs_df_grouped.total_occurrences.fillna(0).astype('int64')
    docs_df_grouped.total_docs = docs_df_grouped.total_docs.fillna(0).astype('int64')
    return docs_df_grouped

def get_counts_from_json(year):
    """ Reads a json file containing total phrase and doc freqs for each year and returns the total
    no. of phrases in the year in the arguments, and the total no. of documents in the year.""" 
    # Read the Json file which has the monthly total phrases and documents -- 2 Json objects in a 
    # json array. Assign each object to a dictionary.
    with open('phrase_urls_and_docs_yearly.json', 'r') as file:
        json_array = json.load(file)
    # json_array is a list of 2 dicts (1st -> phrase freq, 2nd -> doc_freq).
    yearly_phrases_total = json_array[0]
    yearly_docs_total = json_array[1]
    # Get the 2 frequencies for the current year from the 2 dictionaries.
    current_year_phrases_freq = yearly_phrases_total.get(year)
    current_year_docs_freq = yearly_docs_total.get(year)
    return current_year_phrases_freq, current_year_docs_freq

def grouped_dataframes_for_year(year):
    """ Takes a year as argument, queries the Solr index nounphrases_wikipedia on the published_date field using
    this year (it is a DateRange field in Solr) and gets the documents in JSON form. It also converts the 
    result into a Pandas dataframe, groups the data frame by phrase and calculates sum and count (total
    and unique occurrences: unique occurrences = no. of docs). It finally normalizes these 2 columns
    and returns the resulting dataframe"""
    docs = search_solr_parse_json(year, 'nounphrases_wikipedia', 'published_date')
    docs_df = dataframe_from_solr_results(docs)
    grouped_docs_df = group_by_wikipedia_url(docs_df)
    # Normalize by dividing the total_occurrences and total_docs by the total no. of phrases and total no.
    # of docs respectively, and multiplying by 100 to convert to a percentage.
    normalized_df = normalize_dataframes(grouped_docs_df, year)
    return normalized_df

def normalize_dataframes(grouped_df, year):
    """ Takes a dataframe which has been grouped by phrase and aggregated, and normalizes the
    total_occurrences and total_docs columns by dividing them by the total no. of phrases or total no. of
    docs in the relevant year and multiplying by 100 to create percentages.
    The columns in the data frame which is returned is renamed to percentage_occurrences and percentage_docs
    """
    # Get the no. of phrases and documents in the year in the arguments
    wikipedia_url_count, doc_count = get_counts_from_json(year)
    grouped_df['percentage_occurrences'] = grouped_df['total_occurrences'] * 100 / wikipedia_url_count
    grouped_df['percentage_docs'] = grouped_df['total_docs'] * 100 / doc_count

    # Drop the total occurrences and total docs columns, they are no longer necessary
    grouped_df.drop(['total_occurrences', 'total_docs'], axis=1, inplace=True)
    return grouped_df

def join_dfs_on_wikipedia_url(start_df, end_df, start_year):
    """ Takes dataframes pertaining to the start year (2007-2016) and the end year (always 2017), both of which have
    already been grouped, and joins them on the index (phrase) using an inner join. It also creates 2 new
    columns by subtracting the  total and unique phrases in the start year's df from the corresponding values in the
    end year's df. Both the dfs have the index 'wikipedia_url' and the columns 'percentage_occurrences' and 'percentage_docs'
    The columns 'total_occurrences_diff' and 'doc_differences' are added to the output."""
    start_year = '_{}'.format(start_year)
    joined_df = pd.merge(start_df, end_df, how='inner', left_index=True, right_index=True, suffixes=[start_year, '_2017'])
    # The column name for percentage occurrences and percentage docs for the start year should be stored for the later
    # subtractions.
    start_percentage_occurrences_col = 'percentage_occurrences{}'.format(start_year)
    start_percentage_docs_col = 'percentage_docs{}'.format(start_year)
    joined_df['total_occurrences_diff'] = joined_df['percentage_occurrences_2017'] - joined_df[start_percentage_occurrences_col]
    joined_df['doc_differences'] = joined_df['percentage_docs_2017'] - joined_df[start_percentage_docs_col]
    return joined_df

def join_additional_dfs(joined_df, year_dataframe_dict):
    """ Joins each of the dataframes in the year_dataframe_dict (dict with keys=years, values=dfs) with joined_df, which
    has percentage_occurrences and percentage_docs for one year (which is not in year_dataframe_dict, percentage_occurrences
    and percentage_docs for 2017, and 2 more columns total_occurrences_diff, doc_differnces (differences in phrase freq and phrase
    doc freq between 2017 and that one year). Only the 9 years' (in year_dataframe_dict) % doc frequency columns are kept in
    the end (the % phrase frequency columns are removed) """
    # As dict is mutable, take a copy and work on that.
    other_years_dict = copy.deepcopy(year_dataframe_dict)
    for year, year_df in other_years_dict.items():
        percent_docs_with_suffix = 'percentage_docs_{}'.format(year)
        year_df.drop('percentage_occurrences', axis=1, inplace=True)
        year_df.rename(columns={'percentage_docs': percent_docs_with_suffix}, inplace=True)
        # Left Join with existing joined_df (no need of suffix as there are no columns with the same name --- already renamed)
        joined_df = pd.merge(joined_df, year_df, how='left', left_index=True, right_index=True)
    return joined_df

def create_sorted_stats_dfs(joined_df):
    """ Takes a dataframe with phrase as index and percentage_occurrences_start, percentage_docs_start (start is a
    year, for e.g. percentage_occurrences_2007, percentage_docs_2007), percentage_occurrences_2017, percentage_docs_2017,
    total_occurrences_diff, doc_differences (2017-start for phrase count and doc count for each phrase), minyear (min. year count for
    the phrase), avgyearcount (avg. over the years), and the totalcount over the whole period as columns, and
    produces 4 dataframes: (i) phrases with positive total_occurrences_diff (in descending order), (ii) phrases with negative
     total_occurrences_diff (in ascending order), (iii) phrases with postive doc_differences (in descending order), (iv) phrases
     with negative doc_differences (in ascending order).
    Note that total_occurrences_diff and doc_differences are expressed in percentages, and not whole numbers. Also, note
    that phrases which don't show a trend (0 difference) are not inserted into any of the 4 dataframes."""

    positive_phrases_diff = joined_df[joined_df.total_occurrences_diff>0]
    positive_phrases_diff = positive_phrases_diff.sort_values(by='total_occurrences_diff', ascending=False)
    negative_phrases_diff = joined_df[joined_df.total_occurrences_diff<0]
    # Ascending order for negative: we want high negative values to be first.
    negative_phrases_diff = negative_phrases_diff.sort_values(by='total_occurrences_diff')
    positive_docs_diff = joined_df[joined_df.doc_differences>0]
    positive_docs_diff = positive_docs_diff.sort_values(by='doc_differences', ascending=False)
    negative_docs_diff = joined_df[joined_df.doc_differences<0]
    # Ascending order for negative: we want high negative values to be first.
    negative_docs_diff = negative_docs_diff.sort_values(by='doc_differences')
    return positive_phrases_diff, negative_phrases_diff, positive_docs_diff, negative_docs_diff

def calc_mean_min_doc_diff(joined_df, year):
    """ Takes a dataframe with doc differences for each year (phrase is index and there are phrase differences columns too),
    calculates the mean and min of the doc differences over all the years. Removes all the doc differences columns except
    for the one corresponding to the argument 'year' and the one corresponding to 2017."""
    # Columns to apply the mean/min calculation on
    year_columns = ['percentage_docs_2007', 'percentage_docs_2008', 'percentage_docs_2009', 'percentage_docs_2010',
                    'percentage_docs_2011', 'percentage_docs_2012', 'percentage_docs_2013', 'percentage_docs_2014',
                    'percentage_docs_2015', 'percentage_docs_2016', 'percentage_docs_2017']
    joined_df['min_docs_years'] = joined_df[year_columns].min(axis=1)
    joined_df['mean_docs_years'] = joined_df[year_columns].mean(axis=1)
    # Drop all the percentage docs columns other than those for 2017 and 'year' -- remove these 2 years from year_columns
    # and use drop on the rest of the list.
    drop_columns = [year_column for year_column in year_columns if not (year_column.endswith('2017') or year_column.endswith(year))]
    joined_df.drop(drop_columns, axis=1, inplace=True)
    return joined_df

def join_phrase_totals_to_df(joined_df, total_phrase_occurrences, total_docs_occurrences):
    """ Joins the total phrase occurrences and the total_docs occurrences dataframes to joined_df, which contains
    phrase as index """
    total_phrase_occurrences.rename(columns={'num_occurrences': 'total_occurrences'}, inplace=True)
    total_docs_occurrences.rename(columns={'num_documents': 'total_documents'}, inplace=True)
    joined_df = pd.merge(joined_df, total_phrase_occurrences, how='left', left_index=True, right_index=True)
    joined_df = pd.merge(joined_df, total_docs_occurrences, how='left', left_index=True, right_index=True)
    return joined_df

def write_to_files(positive_phrases_diff, negative_phrases_diff, positive_docs_diff, negative_docs_diff, subfolder):
    """ Writes the 4 dataframes in the arguments to four tsv files, all of which are inserted into a new subfolder
    (if it doesn't exist)."""
    path_to_insert = "Output/{}".format(subfolder)
    if not os.path.exists(path_to_insert):
        os.makedirs(path_to_insert)
    positive_phrases_diff.to_csv('{}/positive_phrases_differences.tsv'.format(path_to_insert), sep='\t')
    negative_phrases_diff.to_csv('{}/negative_phrases_differences.tsv'.format(path_to_insert), sep='\t')
    positive_docs_diff.to_csv('{}/positive_docs_differences.tsv'.format(path_to_insert), sep='\t')
    negative_docs_diff.to_csv('{}/negative_docs_differences.tsv'.format(path_to_insert), sep='\t')

def main():
    """Main function which gets the frequencies of phrases in 2 years (in dataframes), joins them together, 
    and also joins them to other years' doc frequencies, total phrase and doc frequencies, and finally
    calculates statistics from them."""
    # Create a list of years -- we want to create a dataframe or a pickle pertaining to each year
    years = ['2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017']
    # Pickle names are defined in this dict comprehension from the years list
    pickle_names = {year: 'Pickles/'+ year + '_grouped_wiki_df.pickle' for year in years}
    # Create an empty dictionary which will contain a mapping from year to df (dataframe is either obtained
    # from grouped_dataframes_for_year or from a pickle if a pickle exists).
    year_dataframe_dict = dict()
    try:
        # If pickle already exists, there is no need to create the data frames again.
        for year in years:
            current_pickle = open(pickle_names.get(year), 'rb')
            year_df = pickle.load(current_pickle)
            # Add the year and dataframe to year_dataframe_dict
            year_dataframe_dict[year] = year_df

    except FileNotFoundError:
        # Pickle doesn't exist, we need to go through the whole process
        # Get dataframes based on a Solr year query, run this for each of the years in turn, create pickles
        # for each of the years
        for year in years:
            # Pickle for each year
            year_df = grouped_dataframes_for_year(year)
            year_pickle = open(pickle_names.get(year), 'wb')
            pickle.dump(year_df, year_pickle)
            year_pickle.close()
            # Also, add the year and dataframe to year_dataframe_dict. As the same dict is updated in the try
            # part, it is possible that the current year has already been pickled and is already in the dict. 
            # In this case, the get function on the dict will return None.
            if year_dataframe_dict.get(year) is None:
                year_dataframe_dict[year] = year_df
    # Read the pickles containing dataframes of the total phrase count of all phrases, and the total no. of docs
    # in which they (all phrases) occur
    total_phrase_occurrences = pd.read_pickle('Pickles/wiki_phrase_count_dataframe.pickle')
    total_docs_occurrences = pd.read_pickle('Pickles/wiki_doc_count_dataframe.pickle')

     # Join on the index (phrase) -- join the end year dataframe (always 2017) with each of the start years in turn
    for year in years[:-1]:
        # We don't want to include 2017, so loop is till -1.
        joined_df = join_dfs_on_wikipedia_url(year_dataframe_dict.get(year), year_dataframe_dict.get('2017'), year)
        # Reduce the dict to the years other than the year in the for loop and 2017
        other_years_dict = {other_year: df for other_year, df in year_dataframe_dict.items() if other_year not in ['2017', year]}
        # Join additional years doc differences as new columns
        joined_df_enhanced = join_additional_dfs(joined_df, other_years_dict)
        # Calculate min and avg doc differences of each of the individual years' values, get back a dataframe without all these years'
        # columns save for the ones correspodning to 2017 and the current year in the for loop, but with addititonal columns for
        # min and mean doc differences instead.
        joined_with_meanmin_df = calc_mean_min_doc_diff(joined_df_enhanced, year)
        joined_with_totals_df = join_phrase_totals_to_df(joined_with_meanmin_df, total_phrase_occurrences, total_docs_occurrences)
        positive_phrases_diff, negative_phrases_diff, positive_docs_diff, negative_docs_diff = create_sorted_stats_dfs(joined_with_totals_df)
        # Write the dataframes to 4 ouptut files in a subdirectory called 'subfolder' based on the current start year in the loop
        subfolder = "WIKI_{}_2017".format(year)
        write_to_files(positive_phrases_diff, negative_phrases_diff, positive_docs_diff, negative_docs_diff, subfolder)

if __name__ == '__main__':
    main()