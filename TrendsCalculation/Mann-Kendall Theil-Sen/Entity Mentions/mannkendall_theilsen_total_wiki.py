""" This module is used to create a data frame with all phrases which occur at lest 100 times in the whole corpus (the
common words which occur 50000 times or more are later discarded), gets the percentage of occurrences they occur in each
year from 2007 to 2017, and if they occur in at least 3 of these years, calculates the Mann Kendall and Theil Sen
statistics. These statistics calculate the strength of a trend, and 5 files are written to which have: 1. Positive
Mann-Kendall Z, 2. Negative Mann Kendall Z, 3. Non-Trending wiki urls by Mann-Kendall, 4. positive Theil Sen
slope, 5. negative Theil Sen slope
Note: it expects pickles produced by yearly_wiki_pickle.py to be present"""

import os
import copy
from collections import OrderedDict
from scipy import stats
import numpy as np
import pandas as pd
pd.options.display.max_rows = 200
# Import module by Michael Schramm for calculating MK statistic
import mk_test

def get_total_df_from_pickles():
    """ Reads pickle which contain all the phrases in the corpus and the no. of docs in which they occur/their total
    frequency, removes phrases starting with special characters, and which don't contain a letter, and then joins
    the docs and total occurrences dataframes to produce one dataframe, which is returned."""
    total_phrase_occurrences = pd.read_pickle('Pickles/wiki_phrase_count_dataframe.pickle')
    #total_phrase_occurrences = total_phrase_occurrences[total_phrase_occurrences['num_occurrences']>100]
    #total_phrase_occurrences.sort_values(by='num_occurrences', ascending=False).head(10)
    total_doc_occurrences = pd.read_pickle('Pickles/wiki_doc_count_dataframe.pickle')

    #total_doc_occurrences.sort_values(by='num_documents', ascending=False).head(200)
    total_phrase_occurrences.rename(columns={'num_occurrences': 'total_occurrences'}, inplace=True)
    total_doc_occurrences.rename(columns={'num_documents': 'total_documents'}, inplace=True)
    joined_df = pd.merge(total_phrase_occurrences, total_doc_occurrences, how='inner', left_index=True, right_index=True)
    joined_df = drop_common_rare_phrases(joined_df)
    return joined_df

def drop_common_rare_phrases(joined_df):
    """ Takes the joined dataframe which contains all urls with their total occurrences in the corpus, total no. of
    docs in which they occur, and drops urls which are common (>50000 total occurrences), or which start with a special
    character. Rare urls (less than 200 total occurrences) are also removed. Returns smaller dataframe. """
    # Drop rows which have total_occurrences greater than 50000: common urls
    #joined_df.drop(joined_df[joined_df.total_occurrences>50000].index, inplace=True)
    joined_df.drop(joined_df[joined_df.total_occurrences<100].index, inplace=True)
    return joined_df

year_columns = ['percentage_occurrences_2007', 'percentage_occurrences_2008', 'percentage_occurrences_2009', 'percentage_occurrences_2010',
                'percentage_occurrences_2011', 'percentage_occurrences_2012', 'percentage_occurrences_2013', 'percentage_occurrences_2014',
                'percentage_occurrences_2015', 'percentage_occurrences_2016', 'percentage_occurrences_2017']

def join_additional_dfs(joined_df, year_dataframe_dict):
    """ Joins each of the dataframes in the year_dataframe_dict (dict with keys=years, values=dfs) with joined_df, which
    has total_occurrences and total_docs of the phrases over the whole corpus. Some phrases have already been removed in
    a previous step, so this is a reduced data frame. The 11 years' percentage occurrences for all the phrases are present in
    year_dataframe_dict, and these are left joined  at the end (the % phrase frequency columns are removed) """
    # As dict is mutable, take a copy and work on that.
    other_years_dict = copy.deepcopy(year_dataframe_dict)
    for year, year_df in other_years_dict.items():
        percent_occurrences_with_suffix = 'percentage_occurrences_{}'.format(year)
        year_df = year_df.drop('percentage_docs', axis=1)
        year_df = year_df.rename(columns={'percentage_occurrences': percent_occurrences_with_suffix})
        # Left Join with existing joined_df (no need of suffix as there are no columns with the same name --- already renamed)
        joined_df = pd.merge(joined_df, year_df, how='left', left_index=True, right_index=True)
    joined_df.fillna(0, inplace=True)
    return joined_df

def remove_more_urls_and_sort(ready_for_stats_df, year_columns):
    """ Sorts the df with 11 years' percentage occurrences, total and doc frequency in the whole corpus, and removes urls which
    have not occurred in at least 3 years. years_columns is a list with the column names for the 11 years. """
    # Min. no of years in which a phrase has to have occurred for it to be admitted to the final data set = 3
    min_years = 3
    # Drop all phrases which don't occur in at least 3 years
    print(ready_for_stats_df.head())
    ready_for_stats_df = ready_for_stats_df.drop(ready_for_stats_df[ready_for_stats_df[year_columns].astype(bool).sum(axis=1)<min_years].index)
    ready_for_stats_df = ready_for_stats_df.sort_values(by='total_occurrences', ascending=False)
    return ready_for_stats_df

def findtheilsen(row):
    """ Calculates the Theil Sen slope for one row which is passed from the Pandas df using apply, returns Theil slope, median intercept"""
    sen = stats.theilslopes(row, np.arange(len(row)), 0.95)
    return [sen[0], sen[2], sen[3]]

def calculate_theilsen_slope(df, year_columns):
    """ Calculates the Theil sen slope of 11 years' total frequencies for each phrase, and 95% confidence intervals. These are added as columns
    to the dataframe"""
    df[['theilsen_slope', 'theilsen_lower_CI', 'theilsen_upper_CI']] = df[year_columns].apply(findtheilsen, axis=1, result_type='expand')
    #print(df[['theilsen_slope', 'theilsen_lower_CI', 'theilsen_upper_CI']].head(75))
    return df

def findmannkendall(row):
    """ Calculates the Mann Kendall statistic for one row which is passed from the Pandas df using apply, returns Theil slope, median intercept"""
    trend_type, trendexists_or_not, p_value, mk_z = mk_test.mk_test(row)
    return [mk_z, trendexists_or_not, p_value, trend_type]

def calculate_mannkendall_statistic(df, year_columns):
    """ Calculates the Mann Kendall statistic of 11 years' total frequencies for each phrase at alpha = 0.05. The Mann Kendall Z is added as a
    column, as is a column which indicates the type of trend, and the associated p-value."""
    df[['mannkendall_z', 'mannkendall_trendexists_or_not', 'mann_kendall_pvalue', 'trend_type_mk']] = df[year_columns].apply(
                                           findmannkendall, axis=1, result_type='expand')
    # print(df[['mannkendall_z', 'mannkendall_trendexists_or_not', 'mann_kendall_pvalue', 'trend_type_mk']].head(75))
    return df

def create_sorted_stats_dfs(df):
    """ Takes a dataframe with phrase as index, percentage occurrences of all years, 3 Mann-Kendall statistic-related columns, and
    3 Theil-Sen slope-related columns, produces 5 dataframes: (i) phrases with increasing trend (in descending order of
    Mann-Kendall Z), (ii) phrases with decreasing trend (in ascending order, i.e. high negative values first)
    of Mann-Kendall Z), (iii) phrases which show no trend according to Mann Kendall (p-value>0.05), (iv) phrases with postive
    theil-sen slope (in descending order), (v) phrases with negative theil-sen slope (in ascending order).
    """
    increasing_mk = df[df.trend_type_mk=='increasing']
    increasing_mk = increasing_mk.sort_values(by='mannkendall_z', ascending=False)
    decreasing_mk = df[df.trend_type_mk=='decreasing']
    # Ascending order for negative: we want high negative values to be first.
    decreasing_mk = decreasing_mk.sort_values(by='mannkendall_z')
    notrend_mk = df[df.trend_type_mk=='no trend']
    # Ascending order by pvalue: values closer to 0.05 first
    notrend_mk = notrend_mk.sort_values(by='mann_kendall_pvalue')

    positive_theilsen = df[df.theilsen_slope>0]
    positive_theilsen = positive_theilsen.sort_values(by='theilsen_slope', ascending=False)
    negative_theilsen = df[df.theilsen_slope<0]
    negative_theilsen = negative_theilsen.sort_values(by='theilsen_slope')
    return increasing_mk, decreasing_mk, notrend_mk, positive_theilsen, negative_theilsen

def write_to_files(increasing_mk, decreasing_mk, notrend_mk, positive_theilsen, negative_theilsen, subfolder):
    """ Writes the 4 dataframes in the arguments to four tsv files, all of which are inserted into a new subfolder
    (if it doesn't exist)."""
    path_to_insert = "Output/{}".format(subfolder)
    if not os.path.exists(path_to_insert):
        os.makedirs(path_to_insert)
    increasing_mk.to_csv('{}/increasing_mannkendall.tsv'.format(path_to_insert), sep='\t')
    decreasing_mk.to_csv('{}/decreasing_mannkendall.tsv'.format(path_to_insert), sep='\t')
    notrend_mk.to_csv('{}/no_trend_mannkendall.tsv'.format(path_to_insert), sep='\t')
    positive_theilsen.to_csv('{}/positive_theilsen.tsv'.format(path_to_insert), sep='\t')
    negative_theilsen.to_csv('{}/negative_theilsen.tsv'.format(path_to_insert), sep='\t')

def main():
    """ Main function which calls other functions to create a data frame from multiple pickles/read from an existing dataframe
    and to call functions to calculate the Mann Kendall and Theil sen statistics."""
    # Define a list with column names for the percentage occurrences for each year
    year_columns = ['percentage_occurrences_2007', 'percentage_occurrences_2008', 'percentage_occurrences_2009', 'percentage_occurrences_2010',
                'percentage_occurrences_2011', 'percentage_occurrences_2012', 'percentage_occurrences_2013', 'percentage_occurrences_2014',
                'percentage_occurrences_2015', 'percentage_occurrences_2016', 'percentage_occurrences_2017']
    try:
        ready_for_stats_df = pd.read_pickle('Pickles/ready_for_kendall_theil_occurrences_wiki.pickle')
        print(ready_for_stats_df.head())
        ready_for_stats_df = remove_more_urls_and_sort(ready_for_stats_df, year_columns)
        df_with_mannkendall = calculate_mannkendall_statistic(ready_for_stats_df, year_columns)
        df_with_theilsen_and_mk = calculate_theilsen_slope(df_with_mannkendall, year_columns)
        print(df_with_theilsen_and_mk.head())
        # Split df into 4 dfs: ones with increasing and decreasing Theil Sen slope resp, and ones with increasing and decreasing
        # Mann-Kendall trend resp.
        increasing_mk, decreasing_mk, notrend_mk, positive_theilsen, negative_theilsen = create_sorted_stats_dfs(df_with_theilsen_and_mk)
        # Write to files
        subfolder = 'mannkendall_and_theilsen_wikipedia_TOTALFREQ'
        write_to_files(increasing_mk, decreasing_mk, notrend_mk, positive_theilsen, negative_theilsen, subfolder)

    except FileNotFoundError:
        # Pickle doesn't exist
        years = ['2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017']
        pickle_names = OrderedDict((year,'Pickles/'+ year + '_grouped_wiki_df.pickle') for year in years)
        year_dataframe_dict = OrderedDict()
        for year in years:
            year_df = pd.read_pickle(pickle_names.get(year))
            # Add the year and dataframe to year_dataframe_dict
            year_dataframe_dict[year] = year_df
        total_df = get_total_df_from_pickles()
        # Join to each of the yearly pickled dataframes
        ready_for_stats_df = join_additional_dfs(total_df, year_dataframe_dict)
        print(ready_for_stats_df.head())
        pd.to_pickle(ready_for_stats_df, 'Pickles/ready_for_kendall_theil_occurrences_wiki.pickle')
        ready_for_stats_df = remove_more_urls_and_sort(ready_for_stats_df, year_columns)
        df_with_mannkendall = calculate_mannkendall_statistic(ready_for_stats_df, year_columns)
        df_with_theilsen_and_mk = calculate_theilsen_slope(df_with_mannkendall, year_columns)
        # Split df into 4 dfs: ones with increasing and decreasing Theil Sen slope resp, and ones with increasing and decreasing
        # Mann-Kendall trend resp.
        increasing_mk, decreasing_mk, notrend_mk, positive_theilsen, negative_theilsen = create_sorted_stats_dfs(df_with_theilsen_and_mk)
        # Write to files
        subfolder = 'mannkendall_and_theilsen_wikipedia_TOTALFREQ'
        write_to_files(increasing_mk, decreasing_mk, notrend_mk, positive_theilsen, negative_theilsen, subfolder)


if __name__ == '__main__':
    main()