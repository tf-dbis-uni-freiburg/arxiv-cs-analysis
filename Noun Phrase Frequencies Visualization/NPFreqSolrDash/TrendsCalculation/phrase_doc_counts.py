""" This module contains a function which creates pickles of 2 dataframes -- one containing the 
no. of times each phrase occurs in all 90278 NP Files,and the second containing the no. of
 documents (papers) in which they occur"""

import os
import copy
from collections import Counter
import pandas as pd
from glob import iglob

def create_phrase_doc_counters():
    """ Counts the no. of times each phrase occurs in all 90278 NP Files, and the no. of
     documents (papers) in which they occur, returns 2 counters: phrase_counter and
     document_counter"""
    folderpath = '/home/ashwath/Files/NPFiles'
    # Create an empty counter and update counts for phrases in each file inside the for loop.
    phrase_counter = Counter()
    # Create another counter for unique counts of phrases (1 per file), this counts the no. of 
    # documents in which the phrase appears.
    document_counter = Counter()
    for filepath in iglob(os.path.join(folderpath, '*.nps.txt')):
        with open(filepath, "r") as file:
            # Line is tab-separated (phrase, start, end). We want only phrase
            # Don't add useless phrases to list 'phrases'. Use a generator
            # expression instead of a list comprehension
            phrases = (line.split("\t")[0].lower().strip() for line in file 
                if line.split("\t")[0].lower().strip() != "")
            temp_phrase_counter = Counter(phrases)
            # Create a temp. counter which has 1 for all phrases in the current
            # file (current iteration). '' keys are set to count 0.
            temp_doc_counter = copy.deepcopy(temp_phrase_counter)
            for np in temp_phrase_counter.keys():
                temp_doc_counter[np] = 1 if np!='' else 0
            phrase_counter.update(temp_phrase_counter)
            document_counter.update(temp_doc_counter)

    return phrase_counter, document_counter

def counter_to_dataframe(phrase_counter, document_counter):
    """ Converts the phrase counter and the document counter into Pandas dataframes, sorts the dataframes
    by the counter column, and returns them"""
    phrases_df = pd.DataFrame.from_dict(phrase_counter, orient='index', columns=['num_occurrences'])
    phrases_df.index.name = 'phrase'
    phrases_df = phrases_df.sort_values(by='phrase', ascending=False)
    docs_df = pd.DataFrame.from_dict(document_counter, orient='index', columns=['num_documents'])
    docs_df.index.name = 'phrase'
    docs_df = docs_df.sort_values(by='phrase', ascending=False)
    return phrases_df, docs_df

def main():
    """ Main function calls the function to create the counters, converts the counters to Pandas dfs by
    calling another function, and then pickles the dataframes. """
    phrase_counter, document_counter = create_phrase_doc_counters()
    phrases_df, docs_df = counter_to_dataframe(phrase_counter, document_counter) 
    phrases_df.to_pickle('Pickles/total_phrase_count_dataframe.pickle')
    docs_df.to_pickle('Pickles/total_doc_count_dataframe.pickle')

if __name__ == '__main__':
    main()
