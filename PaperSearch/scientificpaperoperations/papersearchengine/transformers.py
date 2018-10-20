from collections import Counter
import pandas as pd
import nltk
from sklearn.base import BaseEstimator, TransformerMixin

# Build a custom estimator for POS tags
class PosTagMatrix(BaseEstimator, TransformerMixin):
    #normalize = True - divide all values by a total number of tags in the sentence
    #tokenizer - take a custom tokenizer function    
    def __init__(self, tokenizer = lambda x:x.split(), normalize=False):
        self.tokenizer = tokenizer
        self.normalize = normalize
        
    # Helper function --> tokenize and count POS
    def pos_func(self, sentence):
        return Counter(tag for word, tag in nltk.pos_tag(self.tokenizer(sentence)) if tag.startswith(('VB', 'NN', 'RB', 'MD')))
    
    # Dummy fit, only transform is important
    def fit(self, X, y=None):
        return self
    
    def transform(self, X):
        X_tagged = X.apply(self.pos_func).apply(pd.Series).fillna(0)
        X_tagged['n_tokens'] = X_tagged.apply(sum, axis=1)
        if self.normalize:
            X_tagged = X_tagged.divide(X_tagged['n_tokens'], axis=0)
            
        return X_tagged
    
class TextSelector(BaseEstimator, TransformerMixin):
    """
    Transformer to select a single column from the data frame to perform additional transformations on
    Use on text columns in the data
    """
    def __init__(self, key):
        self.key = key

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        return X[self.key]
    
class NumberSelector(BaseEstimator, TransformerMixin):
    """
    Transformer to select a single column from the data frame to perform additional transformations on
    Use on numeric columns in the data
    """
    def __init__(self, key):
        self.key = key

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        return X[[self.key]]
