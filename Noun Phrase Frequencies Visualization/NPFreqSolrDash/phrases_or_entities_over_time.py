""" This module is used to visualize the monthly doc frequencies (no. of docs in which a phrase is present per month) and
phrase frequencies (no. of times a phrase is present per month) of noun phrase(s) chosen by the user in a Dash user interface.
A Solr query is made for the query/queries, results are aggregated monthly, and converted into percentage of phrases/docs in 
the month by dividing by the total docs/phrases in each month (these are obtained from a json file built for that purpose in
another module.  """
import requests
import sys
import pandas as pd
import json
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go
import base64

# Import programs which produce 4 different dataframes: phrases monthly, phrases yearly, entity mentions monthly, 
# entity mentions yearly.
import nounphrase_visualization_monthly as npvm
import nounphrase_visualization_yearly as npvy
import entity_mentions_visualization_monthly as emvm
import entity_mentions_visualization_yearly as emvy


# Read the list of suggested noun phrases
#suggestions_df = pd.read_csv('WikidataAlgorithms.tsv', sep='\t', header=None, names=['phrase'])
#print(suggestions_df.head())
#suggestions_list = suggestions_df.phrase.tolist()
#print(suggestions_list)
# Read the centres file and put it in a dataframe. 
years = ['2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017']
# zscores for years are 10 columns, 1st column is cluster number
col_list = ['cluster_number'].extend(years)
centres_df = pd.read_csv('centres_df_50.tsv', sep='\t', names=col_list)
centres_df = centres_df.set_index('cluster_number', drop=True)
# Create a list of cluster names: ['Cluster 0', 'Cluster 1',...]
list_of_clusters = centres_df.index.tolist()

phrases_df = pd.read_csv('cluster_phrase_semicolon_50.txt', sep='\t', names=['cluster_number', 'phrases'])
phrases_df = phrases_df.set_index('cluster_number', drop=True)

def phrases_df_notfound_message(nounphrase):
    """ Takes a noun phrase which is not found in the phrases_df input filef and prints a messages
    saying that it is not found. It also includes suitable styling (in an <h3> tag).
    ARGUMENTS: nounphrase: searched noun phrses
    RETURNS: a html h5 message with a message listing the terms not found"""

    return html.H5('Noun phrases not found: {}.'.format(notfound),
            style={'color': colours['text']}
            )

app = dash.Dash(__name__)

# Add the default Dash CSS, and some custom (very simple) CSS to remove the undo button
# app.css.append_css({'external_url': 'https://www.jsdelivr.com/package/npm/normalize.css'})
#app.css.append_css({'external_url': 'https://unpkg.com/sakura.css/css/sakura.css'})
app.css.append_css({'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'})
app.css.append_css({'external_url': 'https://rawgit.com/lwileczek/Dash/master/undo_redo5.css'})
#app.css.append_css({'external_url': '/assets/reset.css'})
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

# White Logo for black background
#image_filename = 'assets/GitHub_Logo_White.png' GitHub_Logo
# Black logo for white background
image_filename = 'assets/GitHub_Logo.png'
encoded_image = base64.b64encode(open(image_filename, 'rb').read())
app.layout = html.Div(style={'backgroundColor': colours['background'],
                             'height':'100vh', 'width': '100%'},
                      children=[ 
    html.A(html.Img(src='data:image/png;base64,{}'.format(encoded_image.decode()), style={'width':'10%', 'height': '9%', 'float': 'right'}), 
       href='https://github.com/tf-dbis-uni-freiburg/arxiv-cs-analysis', target='_blank'),
    html.H2(children='Scientific Trend Miner',
            style={
                'textAlign': 'center',
                'color': colours['text']
            }
    ),
    html.P(children='This demo allows the user to see the trends in noun phrases or Wikipedia entities either monthly or yearly. It also allows the user'
        'to find which "cluster" a noun phrase is in and get the trend of that cluster across years, along with the other words in that cluster.' 
        'Finally, the user can select a cluster (after looking'
        'at the graph of trends of all clusters which is visible by clicking the radio button "Clusters" and look at the words from that cluster',
            style={
                'textAlign': 'center',
                'color': colours['text'],
                'fontSize': '1.1em',
            }
    ),

    html.Label(id='setlabel',
               style={
                'textAlign': 'left',
                'color': colours['text'],
                'fontSize': '1.4em',
                'margin-left': '1%'
            }, className='setlabel'),

    dcc.Input(id='npinput1-state', value='', type='text', style={'width': '75%', 'margin-left': '1%'}),
    html.Div([
        html.Label(children='Or', id='setorlabel',
               style={
                'textAlign': 'left',
                'color': colours['text'],
                'fontSize': '1.4em',
                'margin-left': '1%'
            }, className='setorlabel'),
        dcc.Dropdown(
        id='cluster_dropdown',
        className='cluster_dropdown',
        placeholder='Choose a cluster to get phrases from that cluster')], id='dropdown_div', style={'display': 'none'}),

    html.Div([
        html.Div([
        html.Label('Using:',
               style={
                'textAlign': 'left',
                'color': colours['text'],
                'fontSize': '1.4em'
            }),

        dcc.RadioItems(
                id='type_of_term',
                options=[{'label': i, 'value': i} for i in ['Noun phrases', 'Wikipedia entities', 'Clusters']],
                value='Noun phrases',
                style= {
                    'color': colours['text'],
                    'fontSize': '1.4em'
                },
                labelStyle={'display': 'inline-block'}
            )

        ],  style={'width': '50%', 'margin-left': '1%', 'float':'left'}),
        html.Div([ 
        html.Label('Time interval: ',
               style={
                'textAlign': 'left',
                'color': colours['text'],
                'fontSize': '1.4em'
            }),
    

         dcc.RadioItems(
                id='time_period',
                options=[{'label': i, 'value': i} for i in ['monthly', 'yearly']],
                value='monthly',
                style= {
                    'color': colours['text'],
                    'fontSize': '1.4em'
                },
                labelStyle={'display': 'block'}
            )
        ], style={'margin-right': '1%', 'float': 'left'}, id='timeinterval_div'),
        

        ], style={'width': '100%', 'overflow': 'hidden'}),  
    
    #html.Button(id='submit-button', n_clicks=0, children='Submit', style={'margin-top': '2%', 'margin-left': 'auto',
    #                                                 'margin-right': 'auto', 'width': '20%', 'display': 'block'}),
    html.Button(id='submit-button', n_clicks=0, children='Submit', style={'margin-top': '2%', 'margin-left': '1%'}),
    #                                                 'margin-right': 'auto', 'width': '20%', 'display': 'block'}),
    html.Div(id='output3'),
    html.Div(id='output1'),
    html.Div(id='output2')

])

""",
        # Hidden div for clusters
        html.Div([ 
        html.Label('Number of phrases: ',
               style={
                'textAlign': 'left',
                'color': colours['text'],
                'fontSize': '1.4em'
            }),
    
         dcc.RadioItems(
                id='num_phrases',
                options=[{'label': i, 'value': i} for i in ['first 20', 'all']],
                value='first 20',
                style= {
                    'color': colours['text'],
                    'fontSize': '1.4em'
                },
                labelStyle={'display': 'block'}
            )
        ], style={'display': 'none'}, id='numphrases_div'),


html.Div([
        html.Label(children='Or', id='setorlabel',
               style={
                'textAlign': 'left',
                'color': colours['text'],
                'fontSize': '1.4em',
                'margin-left': '1%'
            }, className='setorlabel'),
        dcc.Dropdown(
        id='cluster_dropdown',
        className='cluster_dropdown',
        placeholder='Choose a cluster to get phrases from that cluster)')], id='dropdown_div', style={'display': 'none'}),
"""

@app.callback(
    Output('setlabel', 'children'),
    [Input('type_of_term', 'value'),
    Input('time_period', 'value')])
def set_label(termtype, timeperiod):
    """ Sets label based on the radio buttons selected"""
    label = 'Graph the following concepts (comma-separated, using yearly frequencies):' if termtype == 'Noun phrases' and timeperiod == 'yearly' \
            else 'Graph the following comma-separated noun phrases (monthly frequencies):' if termtype == 'Noun phrases' and timeperiod == 'monthly' \
            else 'Graph the following comma-separated entities (yearly frequencies):' if termtype == 'Wikipedia entities' and timeperiod == 'yearly' \
            else 'Graph the following comma-separated entities (monthly frequencies):' if termtype == 'Wikipedia entities' and timeperiod == 'monthly' \
            else 'Enter a phrase and show its cluster together with its other concepts:'
    return label

'''@app.callback(
    Output('setorlabel', 'children'),
    [Input('type_of_term', 'value')])
def set_or_label(termtype):
    """ Sets OR Label only if the clusters radio button is selected"""
    if termtype == 'Clusters':
        return 'Or'''

@app.callback(
    Output('dropdown_div', 'style'),
    [Input('type_of_term', 'value')])
def show_dropdown(termtype):
    '''Disable dropdown if termtype is not clusters'''
    if termtype == 'Clusters':
        style = {'display': 'block'}
    else:
        style = {'display': 'none'}
        return style


'''
@app.callback(
    Output('numphrases_div', 'style'),
    [Input('type_of_term', 'value'),
    Input('cluster_dropdown', 'value')])
def show_numphrases_radio(termtype, dropdown_val):
    """ Shows the num phrases radio button only if the Clusters radio button is selected and the dropdown box has a value"""
    if termtype == 'Clusters' and dropdown_val!="":
        style = {'margin-left': '1%', 'float': 'left'}   
    else:
        style = {'display': 'none'}
    return style 

@app.callback(
    Output('timeinterval_div', 'style'),
    [Input('type_of_term', 'value')])
def show_timeinterval_radio(termtype):
    """ Shows the num phrases radio button only if the Clusters radio button is selected and the dropdown box has a value"""
    if termtype == 'Clusters':
        style = {'display': 'none'}
    else:
        style = {'width': '50%', 'margin-right': '1%', 'float': 'left'}
        return style
'''

@app.callback(
    Output('cluster_dropdown', 'options'),
    [Input('type_of_term', 'value'),
    Input('dropdown_div', 'style')])
def set_dropdown_options(termtype, dropdown_style):
    """ Sets dropdown only if the clusters radio button is selected"""
    #  IMPORTANT: this wasted a lot of time, dropdown_style is None when it is not set to {display:none}
    if dropdown_style is None:
        if termtype == 'Clusters':
            options=[{'label': cluster, 'value': cluster} for cluster in list_of_clusters]
        else:
            options = []
    else:
        options = []
    return options

@app.callback(
    Output('npinput1-state', 'placeholder'),
    [Input('type_of_term', 'value')])
def set_placeholder(termtype):
    """ Sets input placeholder based on the radio buttons selected"""
    placeholder = 'E.g. search: "machine learning, model validation"' if termtype == 'Noun phrases'\
            else 'E.g. search: "machine learning, model validation": each search term will automatically be converted to http://en.wikipedia.org/wiki/<search_term>' \
            if termtype == 'Entity mentions' else 'E.g. model validation (one phrase only)'
    return placeholder

@app.callback(
    Output('output1', 'children'),
    [Input('type_of_term', 'value'),
    Input('time_period', 'value'),
    Input('submit-button', 'n_clicks')],
    [State('npinput1-state', 'value')])
def create_graph(termtype, timeperiod, n_clicks, input_box):
    """ Wrapped function which takes user input in a text box, and 2 radio buttons, returns the
    appropriate graph if the query produces a hit in Solr, returns an error message otherwise.
    ARGUMENTS: n_clicks: a parameter of the HTML button which indicates it has 
               been clicked
               input_box: the content of the text box in which the  user has 
               entered a comma-separated search query.
               type_of_term: radio button with values 'Wikipedia entities' or 'Noun phrases'
               time_period: radio button with values 'Monthly' or 'Yearly'
    RETURNS: 1 graph (total occurrences) of all terms which have results from 
             Solr, error messages of all terms which don't have results from Solr.
             The 1 graph is generated based on the radio buttons' values. """

    if termtype == 'Noun phrases' and timeperiod == 'monthly':
        # Call function show_graph_total_not_callback which is a normal function, not a decorator
        print("dddddd")
        return npvm.show_graph_unique_not_callback(n_clicks, input_box)
    if termtype == 'Wikipedia entities' and timeperiod == 'monthly':
        return emvm.show_graph_unique_not_callback(n_clicks, input_box)
    if termtype == 'Noun phrases' and timeperiod == 'yearly':
        return npvy.show_graph_unique_not_callback(n_clicks, input_box)
    if termtype == 'Wikipedia entities' and timeperiod == 'yearly':
        return emvy.show_graph_unique_not_callback(n_clicks, input_box)
    if termtype == 'Clusters' and n_clicks>0 and input_box!="":
        # !!! DO NOT modify global variables
        phrases_df_copy = phrases_df.copy()
        # Add a new column which is 1 only for the cluster in which the term in input box is found.
        phrases_df_copy['clusterfound'] = phrases_df_copy['phrases'].apply(lambda x: 1 if x.find(input_box.strip()) != -1 else 0)

        if (phrases_df_copy.clusterfound==0).all():
            return html.H5('Noun phrase "{}" not found. Try searching again!'.format(input_box.strip()),
            style={'color': colours['text']}
            )
        # one_phrase_df will contain only one row
        one_phrase_df = phrases_df_copy.loc[phrases_df_copy.clusterfound==1]
        current_cluster = one_phrase_df.index.values[0]
        current_cluster_message = 'Other noun phrases in same cluster (cluster {}):\n'.format(str(current_cluster))
        current_cluster = 'Cluster {}'.format(current_cluster)
        # Get the list of words using iloc[0] (only one row) and build it into a string with commas (input file had semicolons)
        current_cluster_phrases = ', '.join(one_phrase_df.phrases.iloc[0].split(';'))

        data = [
            go.Scatter(
                x=centres_df.columns, y=centres_df.loc[current_cluster], mode='lines+markers', name=current_cluster)
            ]

        layout = go.Layout(
                    title = 'Time series for the phrase "{}" ({}) over all years'.format(input_box, current_cluster),
                    xaxis = {'title': 'Year', 'titlefont': {'size': 20}, 'tickfont': {'size': 18}},
                    yaxis = {'title': 'z-Score of {}'.format(current_cluster), 'ticksuffix': '%', 'titlefont': {'size': 20}, 'tickfont': {'size': 18}},
                    plot_bgcolor = colours['background'],
                    paper_bgcolor = colours['background'],
                    hovermode = 'closest',
                    font= {
                            'color': colours['text'],
                            'size': 15
                          },
                    showlegend=True,
                    legend = {'font': {'size': 20}}
                    )
        one_cluster_graph = dict(data=data, layout=layout)
        return dcc.Graph(id='onecluster', figure=one_cluster_graph), html.Div([html.H5(current_cluster_message, style={
                'textAlign': 'left',
                'color': colours['text']
                #'fontSize': '1.4em'
            }), html.P(current_cluster_phrases, style={
                'textAlign': 'left',
                'color': colours['text'],
                'fontSize': '1em'
            })], style={'backgroundColor': colours['background'], 'className': 'phrases_div'})


@app.callback(
    Output('output3', 'children'),
    [Input('type_of_term', 'value'),
    Input('time_period', 'value'),
    Input('submit-button', 'n_clicks'),
    Input('cluster_dropdown', 'value')])
def select_cluster_phrases(termtype, timeperiod, n_clicks, dropdown_val):
    """ Wrapped function which takes user input in a text box, and 2 radio buttons, returns the
    appropriate graph if the query produces a hit in Solr, returns an error message otherwise.
    ARGUMENTS: n_clicks: a parameter of the HTML button which indicates it has 
               been clicked
               input_box: the content of the text box in which the  user has 
               entered a comma-separated search query.
               type_of_term: radio button with values 'Wikipedia entities' or 'Noun phrases'
               time_period: radio button with values 'Monthly' or 'Yearly'
    RETURNS: 1 graph (total occurrences) of all terms which have results from 
             Solr, error messages of all terms which don't have results from Solr.
             The 1 graph is generated based on the radio buttons' values. """
    if termtype == 'Clusters' and dropdown_val != "" and dropdown_val is not None:
        phrases_df_copy = phrases_df.copy()
        dropdown_val = dropdown_val.strip()
        cluster_only_number = int(dropdown_val.split()[-1])
        one_phrase_df = phrases_df_copy.loc[cluster_only_number]

        #if (phrases_df_copy.clusterfound==0).all():
        #    return html.H5('Noun phrase "{}" not found. Try searching again!'.format(input_box.strip()),
        #    style={'color': colours['text']}
        #    )
        # one_phrase_df will contain only one row
        current_cluster = dropdown_val
        current_cluster_message = 'Noun phrases in {}:\n'.format(str(dropdown_val))
        # Get the list of words using iloc[0] (only one row) and build it into a string with commas (input file had semicolons)
        current_cluster_phrases = ', '.join(one_phrase_df.phrases.split(';'))

        # Plot the graph for the current cluster as well
        data = [
            go.Scatter(
                x=centres_df.columns, y=centres_df.loc[current_cluster], mode='lines+markers', name=current_cluster)
            ]

        layout = go.Layout(
                    title = 'Time series for {} over all years'.format(current_cluster),
                    xaxis = {'title': 'Year', 'titlefont': {'size': 20}, 'tickfont': {'size': 18}},
                    yaxis = {'title': 'z-Score of {}'.format(current_cluster), 'ticksuffix': '%', 'titlefont': {'size': 20}, 'tickfont': {'size': 18}},
                    plot_bgcolor = colours['background'],
                    paper_bgcolor = colours['background'],
                    hovermode = 'closest',
                    font= {
                            'color': colours['text'],
                            'size': 15
                          },
                    showlegend=True,
                    legend = {'font': {'size': 20}}
                    )
        current_cluster_graph = dict(data=data, layout=layout)

        return dcc.Graph(id='onecluster', figure=current_cluster_graph), html.Div([html.H5(current_cluster_message, style={
                'textAlign': 'left',
                'color': colours['text'],
                #'fontSize': '1.4em'
            }), html.P(current_cluster_phrases, style={
                'textAlign': 'left',
                'color': colours['text'],
                'fontSize': '1em'
            })], style={'backgroundColor': colours['background'], 'className': 'phrases_div'}
            )


@app.callback(
    Output('output2', 'children'),
    [Input('type_of_term', 'value'),
    Input('submit-button', 'n_clicks')],
    [State('npinput1-state', 'value')])
def create_second_graph(termtype, n_clicks, input_box):
    """ Wrapped function which takes user input in a text box, and 2 radio buttons, returns the
    graph for document frequency trends according to clusters
    ARGUMENTS: n_clicks: a parameter of the HTML button which indicates it has 
               been clicked
               input_box: the content of the text box in which the  user has 
               entered a comma-separated search query.
               type_of_term: radio button with values 'Entity mention' or 'Noun phrase'
              ]
    RETURNS: 1 graph (total occurrences) of all terms which have results from 
             Solr, error messages of all terms which don't have results from Solr.
             The 1 graph is generated based on the radio buttons' values. """

    if termtype == 'Clusters':
        data = [
            go.Scatter(
                x=centres_df.columns, y=centres_df[years].loc[cluster], mode='lines+markers', name=cluster)
                for cluster in centres_df.index
            ]

        layout = go.Layout(
                    title = "Document frequency trends of all 10 clusters over years".format(input_box),
                    xaxis = {'title': 'Year', 'titlefont': {'size': 20}, 'tickfont': {'size': 18}},
                    yaxis = {'title': 'z-Score of Cluster', 'ticksuffix': '%', 'titlefont': {'size': 20}, 'tickfont': {'size': 18}},
                    plot_bgcolor = colours['background'],
                    paper_bgcolor = colours['background'],
                    hovermode = 'closest',
                    font= {
                            'color': colours['text'],
                            'size': 15
                          },
                    showlegend=True,
                    legend = {'font': {'size': 20}}
                    )
        cluster_graph = dict(data=data, layout=layout)
        return dcc.Graph(id='clustergraph', figure=cluster_graph)

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port="8053", debug="on")
