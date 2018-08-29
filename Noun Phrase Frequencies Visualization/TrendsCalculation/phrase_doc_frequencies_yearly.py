""" This module is used to create a data frame with all phrases which occur at lest 100 times in the whole corpus (the
common words which occur 50000 times or more are later discarded), gets the percentage of docs they occur in each
year from 2007 to 2017.
Note: it expects pickles produced by yearly_trends.py to be present"""

import os
import copy
from collections import OrderedDict
import pandas as pd
pd.options.display.max_rows = 200

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

year_columns = ['percentage_docs_2007', 'percentage_docs_2008', 'percentage_docs_2009', 'percentage_docs_2010',
                'percentage_docs_2011', 'percentage_docs_2012', 'percentage_docs_2013', 'percentage_docs_2014',
                'percentage_docs_2015', 'percentage_docs_2016', 'percentage_docs_2017']

def join_additional_dfs(joined_df, year_dataframe_dict):
    """ Joins each of the dataframes in the year_dataframe_dict (dict with keys=years, values=dfs) with joined_df, which
    has total_occurrences and total_docs of the phrases over the whole corpus. Some phrases have already been removed in
    a previous step, so this is a reduced data frame. The 11 years' percentage occurrences for all the phrases are present in
    year_dataframe_dict, and these are left joined to the joined datthe end (the % phrase frequency columns are removed) """
    # As dict is mutable, take a copy and work on that.
    other_years_dict = copy.deepcopy(year_dataframe_dict)
    for year, year_df in other_years_dict.items():
        percent_docs_with_suffix = 'percentage_docs_{}'.format(year)
        year_df.drop('percentage_occurrences', axis=1, inplace=True)
        year_df.rename(columns={'percentage_docs': percent_docs_with_suffix}, inplace=True)
        # Left Join with existing joined_df (no need of suffix as there are no columns with the same name --- already renamed)
        joined_df = pd.merge(joined_df, year_df, how='left', left_index=True, right_index=True)
    joined_df.fillna(0, inplace=True)
    return joined_df


def write_to_file(joined_df, subfolder):
    """ Writes the dataframe in the argument to a tsv file, which is inserted into a new subfolder
    (if it doesn't exist)."""
    path_to_insert = "Output/{}".format(subfolder)
    if not os.path.exists(path_to_insert):
        os.makedirs(path_to_insert)
    joined_df.to_csv('{}/yearly_phrases_docs_frequencies.tsv'.format(path_to_insert), sep='\t')

def main():
    """ Main function which calls other functions to create a data frame from multiple pickles/read from an existing dataframe
    and to call functions to calculate the Mann Kendall and Theil sen statistics."""
    # Define a list with column names for the percentage docs for each year
    year_columns = ['percentage_docs_2007', 'percentage_docs_2008', 'percentage_docs_2009', 'percentage_docs_2010',
                    'percentage_docs_2011', 'percentage_docs_2012', 'percentage_docs_2013', 'percentage_docs_2014',
                    'percentage_docs_2015', 'percentage_docs_2016', 'percentage_docs_2017']
    years = ['2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017']
    pickle_names = OrderedDict((year,'Pickles/'+ year + '_grouped_df.pickle') for year in years)
    year_dataframe_dict = OrderedDict()
    for year in years:
        year_df = pd.read_pickle(pickle_names.get(year))
        # Add the year and dataframe to year_dataframe_dict
        year_dataframe_dict[year] = year_df
    total_df = get_complete_df_from_pickles()
    # Join total_df to all the yearly pickled dataframes
    joined_df = join_additional_dfs(total_df, year_dataframe_dict)
    # Sort the df by total documents in which they occur
    print(joined_df['total_documents'].head(100))
    joined_df = joined_df.sort_values(by='total_documents', ascending=False)
    # Write to file
    print(joined_df['total_documents'].head(100))
    subfolder = 'YearAndMonthPercentages'
    write_to_file(joined_df, subfolder)


if __name__ == '__main__':
    main()