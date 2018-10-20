""" Reads the Json file containing monthly counts and writes to a json file containing yearly counts. """
import json
def months_to_year_json():
    """ Reads a json file containing two json objects in a json array (they have monthly phrase freq and doc
    freq), and sums up the counts for each month in a year (for both json arrays) to create a new JSON file
    with 2 new json objects in a json array. These objects have years as keys and the yearly frequencies
    (obtained by summing up monthly freq) as the values (for phrase freq and doc freq) """ 
    # Read the Json file which has the monthly total phrases and documents -- 2 Json objects in a 
    # json array. Assign each object to a dictionary.
    with open('phrase_urls_and_docs_monthly.json', 'r') as file:
        json_array= json.load(file)
    # json_array is a list of 2 dicts (1st -> phrase freq, 2nd -> doc_freq)
    monthly_phrases_total = json_array[0]
    monthly_docs_total = json_array[1]
    # Create a list of years to be used to add values corresponding to all keys in a year
    years = ['2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017']
    yearly_phrases_total = dict()
    yearly_docs_total = dict()
    # Create 
    for year in years:
        yearly_phrases_total[year] = sum([freq for month, freq in monthly_phrases_total.items() if month.startswith(year)])
        yearly_docs_total[year] = sum([freq for month, freq in monthly_docs_total.items() if month.startswith(year)])

    list_for_json = [yearly_phrases_total, yearly_docs_total]
    # Dump the list to a json file (as a json array with 2 json objects: 1st -> yearly phrase freq, 2nd -> yearly doc freq)
    with open('phrase_urls_and_docs_yearly.json', 'w') as outputfile:
        json.dump(list_for_json, outputfile)

if __name__ == '__main__':
    months_to_year_json()