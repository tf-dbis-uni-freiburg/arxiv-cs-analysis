""" This module is used to visualize the monthly doc frequencies (no. of docs in which a phrase is present per month) and
phrase frequencies (no. of times a phrase is present per month) of noun phrase(s) chosen by the user in a Dash user interface.
A Solr query is made for the query/queries, results are aggregated monthly, and converted into percentage of phrases/docs in 
the month by dividing by the total docs/phrases in each month (these are obtained from a json file built for that purpose in
another module.	 """
import requests
import sys
import pandas as pd
import json
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go

def search_solr_parse_json(query, collection, search_field):
    """ Searches the nounphrases collection on 'phrase' (query),
    parses the json result and returns it as a list of dictionaries where
    each dictionary corresponds to a record. 
    ARGUMENTS: query, string: the user's query entered in a search box
               (if it is comma-separated, only one part of the query is sent
               to this function).
               collection: the Solr collection name (=nounphrases)
               search_field: the Solr field which is queried (=phrase)
    RETURNS: docs, list of dicts: the documents (records) returned by Solr 
             AFTER getting the JSON response and parsing it."""
    solr_url = 'http://localhost:8983/solr/' + collection + '/select'
    # Exact search only
    query = '"' + query + '"'
    # for rows, pass an arbitrarily large number.
    url_params = {'q': query, 'rows': 100000, 'df': search_field}
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
    the JSON results from Solr, converts it into a dataframe, and keeps only the 4
    important columns (discards _version_ and id, and also phrase, keeps published_date,
    num_occurrences and arxiv_identifier). Finally, it makes sure that the published_date
    is the new index.
    ARGUMENTS: documents_list, list of dicts: list of documents (records) returned
               by Solr for one search query
    RETURNS: docs_df, Pandas dataframe, the Solr results converted into a Pandas
             dataframe with index=published_date, columns=arxiv_identifier, num_occurrences"""
    docs_df = pd.DataFrame(documents_list)
    # Remove phrase too, as all the rows will have the same value
    # (the solr query field was phrase).
    docs_df.drop(['_version_', 'id', 'phrase'], axis=1, inplace=True)
    # Change the published_date column from Solr's string timestamp format to a pandas
    # datetime object with just dates.
    docs_df.published_date = pd.to_datetime(docs_df.published_date)
    # Remove March 2007 Data as it has only 2 documents, a phrase present in 2 will be present
    # in 100% of the documents in the graph, and will destroy the scale.
    docs_df = docs_df.drop(docs_df.loc[((docs_df.published_date.dt.month==3) & (docs_df.published_date.dt.year==2007))].index)
    # Make sure the published_date is the index. Once it is the index, we don't
    # really need the column any more.
    docs_df.set_index('published_date', inplace=True, drop=True)
    
    return docs_df

def calculate_aggregates_day_wise(docs_df):
    """ Takes a Pandas data frame with index=published_date, cols: num_occurrences and
    arxiv_identifier as input, calculates the no. of unique and total occurrences by
    grouping by published_date and cacluating the count and sum on the column
    num_occurrences. The aggregate results are suitably renamed and the published_date
    index is reset so that it becomes a column in the output dataframe.
    NOT USED CURRENTLY"""
    agg_df = docs_df.groupby('published_date').num_occurrences.agg(['sum','count']).rename(
        columns={'sum':'total_occurrences','count':'unique_occurrences'}).reset_index()
    #agg_df.sort_values(by='total_occurrences', ascending=False)
    return agg_df

def calculate_aggregates(docs_df):
    """ Takes a Pandas data frame with index=published_date, cols: num_occurrences and
    arxiv_identifier as input, calculates the no. of unique and total occurrences by
    grouping by the month and year part of published_date and then calculating the count 
    and sum based on the column num_occurrences. The aggregate results are suitably 
    renamed and the published_date index is reset so that it becomes a column in the
    output dataframe. 2 dataframes (unique counts and total counts) are returned.
    ARGUMENTS: docs_df, Pandas dataframe with index=published_date,
               columns=num_occurrences and arxiv_identifier
    RETURNS: docs_df_total, a Pandas df grouped on published_date month and year, on
             which 'sum' is applied on num_occurrences.
             docs_df_unique, a Pandas df grouped on published_date month and year, on
             which 'count' is applied on num_occurrences.
             IMPORTANT: the returned dfs have sum and count in the same column called
                        num_occurrences, a new sum/count column is not created.
               """
    # Drop arxiv_identifier, we want to group by the published_date index, and
    # aggregate on num_occurrrences.
    docs_df.drop('arxiv_identifier', axis=1, inplace=True)
    # Dataframe 1 takes the sum of num_occurrences after grouping by month (and year)
    docs_df_total = docs_df.groupby(pd.Grouper(freq='1M')).sum()
    # docs_df_total.index has day as well, we keep only month and year
    # Change num_occurrences to int after replacing nan by 0
    docs_df_total.num_occurrences = docs_df_total.num_occurrences.fillna(0).astype('int64')
    # Dataframe 2 takes the count of num_occurrences after grouping by month (and year)
    # This is a monthly documnet frequency
    docs_df_unique = docs_df.groupby(pd.Grouper(freq='1M')).count()
    # Change num_occurrences to int after replacing nan by 0
    docs_df_unique.num_occurrences = docs_df_unique.num_occurrences.fillna(0).astype('int64')
    return docs_df_total, docs_df_unique

def get_percentage_aggregates(docs_df_total, docs_df_unique):
    """ This function takes 2 dataframes -- one has monthly phrase frequencies, the other has
    monthly document frequencies -- and normalizes the values by dividing by total no. of phrases
    in the corresponding months and total no. of documents in the corresponding month respectively
    (and multiplies by 100) to get percentages 
    ARGUMENTS: docs_df_total, a Pandas df grouped on published_date month and year, on
             which 'sum' is applied on num_occurrences.
             docs_df_unique, a Pandas df grouped on published_date month and year, on
             which 'count' is applied on num_occurrences.
    RETURNS: docs_df_total, the data frame in the arguments with an additional field 'percentage_occurrences'
             calculated by dividing the current value for each month by the no. of phrases in that month
             docs_df_unique, the data frame in the arguments with an additional field 'percentage_occurrences'
             calculated by dividing the current value for each month by the no. of docs in that month
    NOTE: The total no. of docs/phrases in each month is present in a json file phrases_and_docs_monthly.json """
    
    # Read the Json file which has the monthly total phrases and documents -- 2 Json objects in a 
    # json array. Assign each object to a dictionary.
    with open('phrases_and_docs_monthly.json', 'r') as file:
        json_array= json.load(file)
    # json_array is a list of 2 dicts.
    monthly_phrases_total = json_array[0]
    monthly_docs_total = json_array[1]
    # For each of the dataframes, create a monthyear column in the format year-month, e.g. 2017-08.
    # This is a string and matches the value from the json file.
    # Create monthyear column as a period object with frequency = every month
    docs_df_total['monthyear'] = docs_df_total.index.to_period('M')
    # Convert the period object to a string
    docs_df_total.monthyear =  docs_df_total.monthyear.astype('str')
    # Create a new column which uses the value in the monthyear string column as a key in the monthly_phrases_total
    # dict, and gets the corresponding value. The no. of occurrencesis divided by this number. The na_action is not
    # strictly necessary, it is just a precaution which inserts NaN if a key (month+year) is not found. Finally, NaNs are
    # produced if the dict value has a 0 (divide by 0). These NaNs are replaced by 0. * 100 because the final result is in %.
    docs_df_total['percentage_occurrences'] = (100 * docs_df_total.num_occurrences / docs_df_total['monthyear']
        .map(monthly_phrases_total, na_action=None)).fillna(0)
    # Repeat the process for docs_df_unique
    docs_df_unique['monthyear'] = docs_df_unique.index.to_period('M')
    # Convert the period object to a string
    docs_df_unique.monthyear =  docs_df_unique.monthyear.astype('str')
    docs_df_unique['percentage_occurrences'] = (100 * docs_df_unique.num_occurrences / docs_df_unique['monthyear']
        .map(monthly_docs_total, na_action=None)).fillna(0)
    return docs_df_total, docs_df_unique

def get_aggregated_data(query):
    """ Function which returns an aggregated function for a valid query and
    None for an invalid one.
    ARGUMENTS: query, string, one of the parts of the user's comma-separated query
    RETURNS: docs_df_total, a Pandas df grouped on published_date month and year, on
             which 'sum' is applied on num_occurrences and then normalized to get a percentage.
             docs_df_unique, a Pandas df grouped on published_date month and year, on
             which 'count' is applied on num_occurrences and then normalized to get a percentage.
    """
    # Get a list of dictinoaries by parsing the JSON results for the search query
    docs = search_solr_parse_json(query, "nounphrases", "phrase")
    if docs == []:
        # No data found
        return None, None
    # Create a pandas dataframe out of the result
    docs_df = dataframe_from_solr_results(docs)
    # Group by published_date, and calculate sum and count of num_occurrences. 
    #These correspond to total_occurrences of a phrase for a date, and unique
    # occurrences of a phrase for a date.
    docs_df_total, docs_df_unique = calculate_aggregates(docs_df)
    docs_df_total, docs_df_unique = get_percentage_aggregates(docs_df_total, docs_df_unique)
    return docs_df_total, docs_df_unique

app = dash.Dash()

# Add the default Dash CSS, and some custom (very simple) CSS to remove the undo button
# app.css.append_css({'external_url': 'https://www.jsdelivr.com/package/npm/normalize.css'})
#app.css.append_css({'external_url': 'https://unpkg.com/sakura.css/css/sakura.css'})
app.css.append_css({'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'})
app.css.append_css({'external_url': 'https://rawgit.com/lwileczek/Dash/master/undo_redo5.css'})
# Black background, blue text
#colours = {
#    'background': '#111111',
#    'text': '#0080A5'
#}   

# White background, blue text
colours = {
    'background': '#ffffff',
    'text': '#0080A5'
}   
app.layout = html.Div(style={'backgroundColor': colours['background'],
                             'height':'100vh', 'width': '100%'},
                      children=[
    html.H2(children='Distribution of Noun phrases over time',
            style={
                'textAlign': 'center',
                'color': colours['text']
            }
    ),
    html.Label('Graph these comma-separated noun phrases: ',
               style={
                'textAlign': 'left',
                'color': colours['text'],
                'fontSize': '1.4em'
            }),
    dcc.Input(id='npinput1-state', value='', type='text'),
    html.Button(id='submit-button', n_clicks=0, children='Submit'),
    html.Div(id='output_total'),
    html.Div(id='output_unique')

])
 
def not_found_message(notfound_list):
    """ Takes a list of elements not found in the Solr index and produces
    an error message for the whole lot of them together, along with suitable
    styling (in an <h3> tag).
    ARGUMENTS: notfound_list: list of user's search terms which are not found
               in the Solr index
    RETURNS: a html h5 message with a message listing the terms not found"""
    notfound_list = ['"' + term.strip().capitalize() + '"' 
                     for term in notfound_list]
    notfound = ', '.join(notfound_list)
    return html.H5('Noun phrases not found: {}.'.format(notfound),
            style={'color': colours['text']}
            )
    
""" Trigger callback to show graph for total occurrences for all the comma-separated
# search terms when n_clicks of the button is incremented """
@app.callback(Output('output_total', 'children'),
              [Input('submit-button', 'n_clicks')],
              [State('npinput1-state', 'value')])
def show_graph_total(n_clicks, input_box):
    """ Wrapped function which takes user input in a text box, returns a graph
    if the query produces a hit in Solr, returns an error message otherwise.
    ARGUMENTS: n_clicks: a parameter of the HTML button which indicates it has 
               been clicked
               input_box: the content of the text box in which the  user has 
               entered a comma-separated search query.
    RETURNS: 1 graph (total occurrences) of all terms which have results from 
             Solr, error messages of all terms which don't have results from Solr."""
    
    # Store the layout with the appropriate title and y axis labels for the graph
    layout_total = go.Layout(
                    title = 'Percentage of occurrences of chosen noun phrase(s) per Month',
                    xaxis = {'title': 'Publication date', 'tickformat': '%b %y', 'tick0': '2007-04-30',
                             'dtick': 'M2', 'range': ['2007-03-25', '2018-01-25']},
                    yaxis = {'title': 'Percentage of phrase occurrences', 'ticksuffix': '%'},
                    plot_bgcolor = colours['background'],
                    paper_bgcolor = colours['background'],
                    barmode = 'stack',
                    hovermode = 'closest',
                    font= {
                            'color': colours['text']
                          },
                    showlegend=True
                    )
    
    if input_box != '':
        # Get the input data: both freq_df dfs will have index= published_date,
        # columns = percentage_occurrences total.
        input_list = input_box.lower().split(',')
        data_list_total = []
        notfound_list = []
        for input_val in input_list:
            # Make sure to strip input_val, otherwise if the user enters a 
            # space after the comma in the query, this space will get sent
            # to Solr.
            freq_df_total, freq_df_unique = get_aggregated_data(input_val.strip())
            if freq_df_total is not None:
                # Plot the graphs, published_date (index) goes on the x-axis,
                # and percentage_occurrences total goes on the y-axis.
                data_list_total.append(go.Bar(
                                    x = freq_df_total.index,
                                    y = freq_df_total.percentage_occurrences,
                                    text = input_val.strip().capitalize(),
                                    opacity = 0.7,
                                    name = input_val.strip().capitalize()
                                    ))

            else:
                # Term not found, append it to the not found list and go to the
                # next term.
                notfound_list.append(input_val)
                
        if data_list_total == []:
            if notfound_list != []:
                # Append the error message for the terms not found in the 
                # Solr index
                return not_found_message(notfound_list)
             
            # One or more of the Solr queries returned a result
        else:
            #graph_total_terms = {'data': data_list_total, 'layout': layout_total}
            graph_total_terms = dict(data=data_list_total, layout=layout_total)
            if notfound_list != []:
                terms_not_found = not_found_message(notfound_list)
                #return terms_not_found, html.Br(),
                return terms_not_found, dcc.Graph(id='totalfreq', figure= graph_total_terms)
                                        
            return html.Br(), dcc.Graph(id='totalfreq', figure= graph_total_terms)

""" Trigger callback to show graph for unique occurrences for all the comma-separated
# search terms when n_clicks of the button is incremented """
@app.callback(Output('output_unique', 'children'),
              [Input('submit-button', 'n_clicks')],
              [State('npinput1-state', 'value')])
def show_graph_unique(n_clicks, input_box):
    """ Wrapped function which takes user input in a text box, returns a graph
    if the query produces a hit in Solr.
    ARGUMENTS: n_clicks: a parameter of the HTML button which indicates it has 
               been clicked
               input_box: the content of the text box in which the  user has 
               entered a comma-separated search query.
    RETURNS: 1 graph (unique occurrences) of all terms which have results 
               from Solr """
    # Store the layout with the appropriate title and y axis labels for the graph
    layout_unique = go.Layout(
                    title = 'Percentage of papers containing chosen noun phrase(s) per Month',
                    xaxis = {'title': 'Publication date', 'tickformat': '%b %y', 'tick0': '2007-04-30',
                             'dtick': 'M2', 'range': ['2007-03-25', '2018-01-25']},
                    yaxis = {'title': 'Percentage of papers with noun phrase', 'ticksuffix': '%'},
                    plot_bgcolor = colours['background'],
                    paper_bgcolor = colours['background'],
                    barmode = 'stack',
                    hovermode = 'closest',
                    font= {
                            'color': colours['text']
                          },
                    showlegend=True
                    )
    
    if input_box != '':
        # Get the input data: both freq_df dfs will have index= published_date,
        # columns = percentage_occurrences unique.
        input_list = input_box.lower().split(',')
        data_list_unique = []
        notfound_list = []
        for input_val in input_list:
            # Make sure to strip input_val, otherwise if the user enters a 
            # space after the comma in the query, this space will get sent
            # to Solr.
            freq_df_total, freq_df_unique = get_aggregated_data(input_val.strip())
            if freq_df_unique is not None:
                # Plot the graphs, published_date (index) goes on the x-axis,
                # and percentage_occurrences (unique) goes on the y-axis.
                data_list_unique.append(go.Bar(
                                    x = freq_df_unique.index,
                                    y = freq_df_unique.percentage_occurrences,
                                    text = input_val.strip().capitalize(),
                                    opacity = 0.7,
                                    name = input_val.strip().capitalize()
                                    ))
            else:
                # Term not found, append it to the not found list and go to the
                # next term.
                notfound_list.append(input_val)
                
        if data_list_unique == []:
            if notfound_list != []:
                # Append the error message for the terms not found in the 
                # Solr index
                return html.Br()
             
            # One or more of the Solr queries returned a result
        else:
            graph_unique_terms = {'data': data_list_unique, 'layout': layout_unique}
            if notfound_list != []:
                return dcc.Graph(id='uniquefreq', figure= graph_unique_terms)
                                        
            return html.Br(), dcc.Graph(id='uniquefreq', figure= graph_unique_terms)

def show_graph_total_not_callback(n_clicks, input_box):
    """ Function which is called by a wrapped function in another module. It 
    takes user input in a text box, returns a graph if the query produces a hit in Solr, 
    returns an error message otherwise.
    ARGUMENTS: n_clicks: a parameter of the HTML button which indicates it has 
               been clicked
               input_box: the content of the text box in which the  user has 
               entered a comma-separated search query.
    RETURNS: 1 graph (total occurrences) of all terms which have results from 
             Solr, error messages of all terms which don't have results from Solr."""
    
    # Store the layout with the appropriate title and y axis labels for the graph
    layout_total = go.Layout(
                    title = 'Percentage of occurrences of chosen noun phrase(s) per Month',
                    xaxis = {'title': 'Publication date', 'tickformat': '%b %y', 'tick0': '2007-04-30',
                             'dtick': 'M2', 'range': ['2007-03-25', '2018-01-25']},
                    yaxis = {'title': 'Percentage of phrase occurrences', 'ticksuffix': '%'},
                    plot_bgcolor = colours['background'],
                    paper_bgcolor = colours['background'],
                    barmode = 'stack',
                    hovermode = 'closest',
                    font= {
                            'color': colours['text']
                          },
                    showlegend=True
                    )
    
    if input_box != '':
        # Get the input data: both freq_df dfs will have index= published_date,
        # columns = percentage_occurrences total.
        input_list = input_box.lower().split(',')
        data_list_total = []
        notfound_list = []
        for input_val in input_list:
            # Make sure to strip input_val, otherwise if the user enters a 
            # space after the comma in the query, this space will get sent
            # to Solr.
            freq_df_total, freq_df_unique = get_aggregated_data(input_val.strip())
            if freq_df_total is not None:
                # Plot the graphs, published_date (index) goes on the x-axis,
                # and percentage_occurrences total goes on the y-axis.
                data_list_total.append(go.Bar(
                                    x = freq_df_total.index,
                                    y = freq_df_total.percentage_occurrences,
                                    text = input_val.strip().capitalize(),
                                    opacity = 0.7,
                                    name = input_val.strip().capitalize()
                                    ))

            else:
                # Term not found, append it to the not found list and go to the
                # next term.
                notfound_list.append(input_val)
                
        if data_list_total == []:
            if notfound_list != []:
                # Append the error message for the terms not found in the 
                # Solr index
                return not_found_message(notfound_list)
             
            # One or more of the Solr queries returned a result
        else:
            #graph_total_terms = {'data': data_list_total, 'layout': layout_total}
            graph_total_terms = dict(data=data_list_total, layout=layout_total)
            if notfound_list != []:
                terms_not_found = not_found_message(notfound_list)
                #return terms_not_found, html.Br(),
                return terms_not_found, dcc.Graph(id='totalfreq', figure= graph_total_terms)
                                        
            return html.Br(), dcc.Graph(id='totalfreq', figure= graph_total_terms)

def show_graph_unique_not_callback(n_clicks, input_box):
    """ Normal function which is called by a wrapped function in another module. It
    takes user input in a text box, returns a graph if the query produces a hit in Solr.
    ARGUMENTS: n_clicks: a parameter of the HTML button which indicates it has 
               been clicked
               input_box: the content of the text box in which the  user has 
               entered a comma-separated search query.
    RETURNS: 1 graph (unique occurrences) of all terms which have results 
               from Solr """
    # Store the layout with the appropriate title and y axis labels for the graph
    layout_unique = go.Layout(
                    title = 'Percentage of papers containing chosen noun phrase(s) per Month',
                    xaxis = {'title': 'Publication date', 'tickformat': '%b %y', 'tick0': '2007-04-30',
                             'dtick': 'M2', 'range': ['2007-03-25', '2018-01-25'], 'titlefont': {'size': 20}, 'tickfont': {'size': 15}},
                    yaxis = {'title': 'Percentage of papers with noun phrase', 'ticksuffix': '%', 'titlefont': {'size': 20}, 'tickfont': {'size': 18}},
                    plot_bgcolor = colours['background'],
                    paper_bgcolor = colours['background'],
                    barmode = 'stack',
                    hovermode = 'closest',
                    font= {
                            'color': colours['text'],
                            'size': 15
                          },
                    showlegend=True,
                    legend = {'font': {'size': 18}}
                    )
    
    if input_box != '':
        # Get the input data: both freq_df dfs will have index= published_date,
        # columns = percentage_occurrences unique.
        input_list = input_box.lower().split(',')
        data_list_unique = []
        notfound_list = []
        for input_val in input_list:
            # Make sure to strip input_val, otherwise if the user enters a 
            # space after the comma in the query, this space will get sent
            # to Solr.
            freq_df_total, freq_df_unique = get_aggregated_data(input_val.strip())
            if freq_df_unique is not None:
                # Plot the graphs, published_date (index) goes on the x-axis,
                # and percentage_occurrences (unique) goes on the y-axis.
                data_list_unique.append(go.Bar(
                                    x = freq_df_unique.index,
                                    y = freq_df_unique.percentage_occurrences,
                                    text = input_val.strip().capitalize(),
                                    opacity = 0.7,
                                    name = input_val.strip().capitalize()
                                    ))
            else:
                # Term not found, append it to the not found list and go to the
                # next term.
                notfound_list.append(input_val)
                
        if data_list_unique == []:
            if notfound_list != []:
                # Append the error message for the terms not found in the 
                # Solr index
                # NOTE: this is a change as it is called as the first graph
                #return html.Br()
                return not_found_message(notfound_list)
             
            # One or more of the Solr queries returned a result
        else:
            graph_unique_terms = {'data': data_list_unique, 'layout': layout_unique}
            if notfound_list != []:
                # This is also a change: terms_not_found is returned
                terms_not_found = not_found_message(notfound_list)
                return terms_not_found, dcc.Graph(id='uniquefreq', figure= graph_unique_terms)
                                        
            return html.Br(), dcc.Graph(id='uniquefreq', figure= graph_unique_terms)
            

    
if __name__ == '__main__':
    app.run_server(host='0.0.0.0')