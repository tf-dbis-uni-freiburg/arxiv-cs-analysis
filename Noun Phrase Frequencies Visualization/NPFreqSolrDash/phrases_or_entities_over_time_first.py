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
centres_df = pd.read_csv('centres_df.tsv', sep='\t', names=col_list)
centres_df = centres_df.set_index('cluster_number', drop=True)

phrases_df = pd.read_csv('cluster_phrase_semicolon.txt', sep='\t', names=['cluster_number', 'phrases'])
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
#app.css.append_css({'external_url': 'https://rawgit.com/lwileczek/Dash/master/undo_redo5.css'})
app.css.append_css({'external_url': '/static/reset.css'})
colours = {
    'background': '#111111',
    'text': '#0080A5'
}   
app.layout = html.Div(style={'backgroundColor': colours['background'],
                             'height':'100vh', 'width': '100%'},
                      children=[
    html.H2(children='Distribution of Noun phrases/Entity Mentions over time',
            style={
                'textAlign': 'center',
                'color': colours['text']
            }
    ),

    html.Label(id='setlabel',
               style={
                'textAlign': 'left',
                'color': colours['text'],
                'fontSize': '1.4em',
                'margin-left': '1%'
            }),

    dcc.Input(id='npinput1-state', value='', type='text', style={'width': '75%', 'margin-left': '1%'}),

    html.Div([
        html.Div([
        html.Label('Type:',
               style={
                'textAlign': 'left',
                'color': colours['text'],
                'fontSize': '1.4em'
            }),

        dcc.RadioItems(
                id='type_of_term',
                options=[{'label': i, 'value': i} for i in ['Noun phrases', 'Entity mentions', 'Clusters']],
                value='Noun phrases',
                style= {
                    'color': colours['text'],
                    'fontSize': '1.4em'
                },
                labelStyle={'display': 'inline-block'}
            )

        ],  style={'width': '50%', 'margin-left': '1%', 'float':'left'}),
        html.Div([ 
        html.Label('Time Period: ',
               style={
                'textAlign': 'left',
                'color': colours['text'],
                'fontSize': '1.4em'
            }),
    
         dcc.RadioItems(
                id='time_period',
                options=[{'label': i, 'value': i} for i in ['Monthly', 'Yearly']],
                value='Monthly',
                style= {
                    'color': colours['text'],
                    'fontSize': '1.4em'
                },
                labelStyle={'display': 'inline-block'}
            )
        ], style={'width': '505', 'margin-right': '1%', 'float': 'left'})

        ], style={'width': '100%', 'overflow': 'hidden'}),  
    
    #html.Button(id='submit-button', n_clicks=0, children='Submit', style={'margin-top': '2%', 'margin-left': 'auto',
    #                                                 'margin-right': 'auto', 'width': '20%', 'display': 'block'}),
    html.Button(id='submit-button', n_clicks=0, children='Submit', style={'margin-top': '2%', 'margin-left': '1%'}),
    #                                                 'margin-right': 'auto', 'width': '20%', 'display': 'block'}),
    html.Div(id='output1'),
    html.Div(id='output2')

])

@app.callback(
    Output('setlabel', 'children'),
    [Input('type_of_term', 'value'),
    Input('time_period', 'value')])
def set_label(termtype, timeperiod):
    """ Sets label based on the radio buttons selected"""
    label = 'Graph these comma-separated noun phrases (yearly frequencies):' if termtype == 'Noun phrases' and timeperiod == 'Yearly' \
            else 'Graph these comma-separated noun phrases (monthly frequencies):' if termtype == 'Noun phrases' and timeperiod == 'Monthly' \
            else 'Graph these comma-separated entity mentions (yearly frequencies):' if termtype == 'Entity mentions' and timeperiod == 'Yearly' \
            else 'Graph these comma-separated entity mentions (monthly frequencies):' if termtype == 'entity mentions' and timeperiod == 'Monthly' \
            else 'Enter a phrase and get similar terms and the distribution of its "cluster"'
    return label

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
               type_of_term: radio button with values 'Entity mention' or 'Noun phrase'
               time_period: radio button with values 'Monthly' or 'Yearly'
    RETURNS: 1 graph (total occurrences) of all terms which have results from 
             Solr, error messages of all terms which don't have results from Solr.
             The 1 graph is generated based on the radio buttons' values. """

    if termtype == 'Noun phrases' and timeperiod == 'Monthly':
        # Call function show_graph_total_not_callback which is a normal function, not a decorator
        return npvm.show_graph_unique_not_callback(n_clicks, input_box)
    if termtype == 'Entity mentions' and timeperiod == 'Monthly':
        return emvm.show_graph_unique_not_callback(n_clicks, input_box)
    if termtype == 'Noun phrases' and timeperiod == 'Yearly':
        return npvy.show_graph_unique_not_callback(n_clicks, input_box)
    if termtype == 'Entity mentions' and timeperiod == 'Yearly':
        return emvy.show_graph_unique_not_callback(n_clicks, input_box)
    if termtype == 'Clusters':
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
                    title = 'Document frequency trends of phrase "{}" over years'.format(current_cluster),
                    xaxis = {'title': 'Year'},
                    yaxis = {'title': 'z-Score of {}'.format(current_cluster), 'ticksuffix': '%'},
                    plot_bgcolor = colours['background'],
                    paper_bgcolor = colours['background'],
                    hovermode = 'closest',
                    font= {
                            'color': colours['text']
                          },
                    showlegend=True
                    )
        one_cluster_graph = dict(data=data, layout=layout)
        return dcc.Graph(id='onecluster', figure=one_cluster_graph), html.Div([html.H5(current_cluster_message, style={
                'textAlign': 'left',
                'color': colours['text'],
                #'fontSize': '1.4em'
            }), html.P(current_cluster_phrases, style={
                'textAlign': 'left',
                'color': colours['text'],
                'fontSize': '1em'
            })], style={'backgroundColor': colours['background'], 'className': 'phrases_div'})

@app.callback(
    Output('output2', 'children'),
    [Input('type_of_term', 'value'),
    Input('submit-button', 'n_clicks')],
    [State('npinput1-state', 'value')])
def create_graph(termtype, n_clicks, input_box):
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
                    xaxis = {'title': 'Year'},
                    yaxis = {'title': 'z-Score of Cluster', 'ticksuffix': '%'},
                    plot_bgcolor = colours['background'],
                    paper_bgcolor = colours['background'],
                    hovermode = 'closest',
                    font= {
                            'color': colours['text']
                          },
                    showlegend=True
                    )
        cluster_graph = dict(data=data, layout=layout)
        return dcc.Graph(id='clustergraph', figure=cluster_graph)

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port="8060", debug="on")