from django.shortcuts import render
from django.http import HttpResponse, Http404, HttpResponseRedirect
from .forms import SearchPapersForm, SearchCitedAuthorsForm, SearchCitedPaperForm, SearchAuthorsForm, SearchMetatitleForm
from .django_paper_search import *

# Create your views here.

def index(request):
    return render(
     request,
    'papersearchengine/index.html',
    )

def phrase_search(request):
    """ Implements  the phrase search by displaying a search form, checking for errors
    and rendering the results in the front-end."""
    # Check if the query field has already been populated. If so, send a get request to
    # the form itself.
    if request.method == 'GET' and request.GET.get('query'):
        form = SearchPapersForm(request.GET)
        # Check if data has been entered already in either query or num_rows
        if form.is_valid():
            # Error handling done by Django. If it's not valid, it just jumps
            # to the render at the end.
            # cleaned is a dict, can be passed in render's context directly if needed
            cleaned = form.cleaned_data
            query = cleaned.get('query')
            numrows = cleaned.get('numrows')
            if numrows is None:
                numrows = 10
            # Render the search results form
            reslist = search_sentences(query, numrows)
            if reslist == []:
                # No results found
                printdict = {'query': query, 'numresults': 0, 'results':[], 'numrows': numrows}
            else:
                results, query, num_rows, num_results = reslist
                printdict = {'query': query, 'numresults': num_results, 'results':results, 'numrows': numrows}

            return render(request, 'papersearchengine/phrasesearchresults.html', 
                          printdict)
    else:
        form=SearchPapersForm()
    # Render empty form       
    return render(request, 'papersearchengine/phrasesearch.html',{'form':form})

def metadatatitle_search(request):
     """ Implements  the metadata title search by displaying a search form, checking for errors
     and rendering the results in the front-end."""
     # Check if the query field has already been populated. If so, send a get request to
     # the form itself.
     if request.method == 'GET' and request.GET.get('query'):
         form = SearchMetatitleForm(request.GET)
         # Check if data has been entered already in either query or num_rows
         if form.is_valid():
             # Error handling done by Django. If it's not valid, it just jumps
             # to the render at the end.
             # cleaned is a dict, can be passed in render's context directly if needed
             cleaned = form.cleaned_data
             query = cleaned.get('query')
             numrows = cleaned.get('numrows')
             if numrows is None:
                 numrows = 10
             results, num_results, num_rows = search_meta_titles(query, numrows)
             if results == []:
                 # No results found
                 printdict = {'query': query, 'numresults': 0, 'results':[], 'numrows': numrows}
             else:
                 results = normalize_results(results)
                 printdict = {'query': query, 'numresults': num_results, 'results':results, 'numrows': numrows}

             return render(request, 'papersearchengine/titlesearchresults.html', 
                           printdict)
     else:
         form=SearchMetatitleForm()
     # Render empty form       
     return render(request, 'papersearchengine/titlesearch.html', {'form':form})

def author_search(request):
     """ Implements  the author search by displaying a search form, checking for errors
     and rendering the results in the front-end."""
     # Check if the query field has already been populated. If so, send a get request to
     # the form itself.
     if request.method == 'GET' and request.GET.get('query'):
         form = SearchAuthorsForm(request.GET)
         # Check if data has been entered already in either query or num_rows
         if form.is_valid():
             # Error handling done by Django. If it's not valid, it just jumps
             # to the render at the end.
             # cleaned is a dict, can be passed in render's context directly if needed
             cleaned = form.cleaned_data
             query = cleaned.get('query')
             numrows = cleaned.get('numrows')
             if numrows is None:
                 numrows = 10
             # Split the query to individual authors and remove spaces and send the authors list to 
             # search_authors. 
             authors = query.split(';')
             authors = [author.strip() for author in authors]
             # Create a display string for the query with ANDs between authors.
             displayauthors = ' AND '.join(authors)
             results, num_results, num_rows = search_authors(authors, numrows)
             if results == []:
                 # No results found
                 printdict = {'query': displayauthors, 'numresults': 0, 'results':[], 'numrows': numrows}
             else:
                 results = normalize_results(results)
                 printdict = {'query': displayauthors, 'numresults': num_results, 'results':results, 'numrows': numrows}

             return render(request, 'papersearchengine/authorsearchresults.html', 
                           printdict)
     else:
         form=SearchAuthorsForm()
     # Render empty form       
     return render(request, 'papersearchengine/authorsearch.html', {'form':form})

def normalize_results(results):
    """ This func normalizes the published date and authors of metadata so that they are displayed in the right format,
    and displays suitable messages if they are not found. """
    for result in results:
        # authors is result[1] and published_date is result[4]
        result[1] = '; '.join(result[1])
        # Strip off timestamp (which solr returns with T00... after 10th character, and display in formtat January 13, 2018 instead
        # of 2018-01-13). Finally, convert the list of dates into a string separated by semicolon and space
        if len(result[4]) == 1:
            result[4] = '; '.join([datetime.datetime.strptime(date[:10], '%Y-%m-%d').strftime('%B %d, %Y') for  date in result[4]])
        else:
            result[4] = '; '.join([datetime.datetime.strptime(date[:10], '%Y-%m-%d').strftime('%B %d, %Y') for  date in result[4]]) + \
                        ' (multiple dates indicate revisions to the paper)'
    return results

def cited_author_serach(request):
     """ Implements  the cited author search by displaying a search form, checking for errors
     and rendering the results in the front-end."""
     # Check if the query field has already been populated. If so, send a get request to
     # the form itself.
     if request.method == 'GET' and request.GET.get('query'):
         form = SearchCitedAuthorsForm(request.GET)
         # Check if data has been entered already in either query or num_rows
         if form.is_valid():
             # Error handling done by Django. If it's not valid, it just jumps
             # to the render at the end.
             # cleaned is a dict, can be passed in render's context directly if needed
             cleaned = form.cleaned_data
             query = cleaned.get('query')
             numrows = cleaned.get('numrows')
             if numrows is None:
                 numrows = 10
             # Render the search results form
             reslist = search_references(query, numrows, 'authors')
             if reslist == []:
                 # No results found
                 printdict = {'query': query, 'numresults': 0, 'results':[], 'numrows': numrows, 
                              'totalcitations': 0, 'uniquecitations': 0}
             else:
                 results, total_citations, unique_citations, num_rows, num_results, query = reslist
                 # Display only the query (remove the proximity symbol etc.)
                 query = query[:query.rfind('"')+1]
                 printdict = {'query': query, 'totalcitations': total_citations, 'uniquecitations': unique_citations, 
                              'results':results, 'numrows': numrows, 'numresults': num_results}

             return render(request, 'papersearchengine/citedauthorsearchresults.html', 
                           printdict)
     else:
         form=SearchCitedAuthorsForm()
     # Render empty form       
     return render(request, 'papersearchengine/citedauthorsearch.html', {'form':form})

def cited_paper_search(request):
     """ Implements  the cited paper search by displaying a search form, checking for errors
     and rendering the results in the front-end."""
     # Check if the query field has already been populated. If so, send a get request to
     # the form itself.
     if request.method == 'GET' and request.GET.get('query'):
         form = SearchCitedPaperForm(request.GET)
         # Check if data has been entered already in either query or num_rows
         if form.is_valid():
             # Error handling done by Django. If it's not valid, it just jumps
             # to the render at the end.
             # cleaned is a dict, can be passed in render's context directly if needed
             cleaned = form.cleaned_data
             query = cleaned.get('query')
             numrows = cleaned.get('numrows')
             if numrows is None:
                 numrows = 10
             # Render the search results form
             reslist = search_references(query, numrows, 'title')
             if reslist == []:
                 # No results found
                 printdict = {'query': query, 'numresults': 0, 'results':[], 'numrows': numrows, 
                              'totalcitations': 0, 'uniquecitations': 0}

             else:
                 results, total_citations, unique_citations, num_rows, num_results, query = reslist
                 # Display only the query (remove the proximity symbol etc.)
                 query = query[:query.rfind('"')+1]
                 printdict = {'query': query, 'totalcitations': total_citations, 'uniquecitations': unique_citations, 
                              'results':results, 'numrows': numrows, 'numresults': num_results}

             return render(request, 'papersearchengine/citedpapersearchresults.html', 
                           printdict)
     else:
         form=SearchCitedPaperForm()
     # Render empty form       
     return render(request, 'papersearchengine/citedpapersearch.html', {'form':form})
