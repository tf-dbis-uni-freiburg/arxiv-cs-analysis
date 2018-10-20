import pandas as pd
import numpy as np
from nltk.corpus import stopwords
import nltk
import re
from collections import Counter

from sklearn.pipeline import Pipeline, FeatureUnion
from sklearn.feature_extraction.text import TfidfTransformer, TfidfVectorizer, CountVectorizer
from sklearn.model_selection import train_test_split, StratifiedKFold, GridSearchCV, cross_validate
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import SGDClassifier
from sklearn.metrics import make_scorer, recall_score, f1_score, accuracy_score, precision_score, confusion_matrix, classification_report
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.externals import joblib

from transformers import PosTagMatrix, TextSelector, NumberSelector

        
def read_corpus_create_X_and_y():
    """ Reads the corpus supplied by Athar, produce and return a dataframe called X which has sentences from the corpus, and a Series y
    which has the sentiments (labels)."""
    filename = 'citation_sentiment_corpus.txt'
    df = pd.read_csv(filename, sep="\t", skiprows=18, names=['col1', 'col2', 'sentiment', 'sentence'], usecols=['sentiment', 'sentence'])
    # Keep the labels and the sentences separate: y is a Series, X is a data frame
    X = df.drop('sentiment', axis=1)
    y = df.sentiment
    # The division of labels is heavily skewed: o (neutral): 7627, p (positive): 829, n (negative): 280
    #print(y.value_counts())
    return X, y

def read_polar_phrases():
    """ Reads a file of positive/negative words, return 2 separate lists of positive and negative words resp."""
    # Open positive and negative polarity list file.
    polarity_df = pd.read_csv('polar_phrases.txt', sep='\t', names=['polar_word', 'polarity'])
    #polarity_df.set_index('polar_word', inplace=True, drop=True)
    negative_polarity_df = polarity_df[polarity_df.polarity==-1]
    positive_polarity_df = polarity_df[polarity_df.polarity==1]
    positive_polarity_words = polarity_df.polar_word.tolist()
    negative_polarity_words = negative_polarity_df.polar_word.tolist()
    return positive_polarity_words, negative_polarity_words

def processing(df, positive_polarity_words, negative_polarity_words):
    """ Does pre-processing on a dataframe"""
    # Remove punctuation using re.sub(pattern, replace, input): \w: letter/num/underscore, \s: space. Also, make everything lowercase
    df['processed'] = df['sentence'].apply(lambda x: re.sub(r'[^\w\s]', '', x.lower()))
    
    # Numerical features: these will need to be scaled in the pipeline
    df['num_negative_words'] = df['processed'].apply(lambda sen: sum([1 if sen.find(word) != -1 else 0 for word in negative_polarity_words]))
    df['num_positive_words'] = df['processed'].apply(lambda sen: sum([1 if sen.find(word) != -1 else 0 for word in positive_polarity_words]))
    return df
    
def create_pipeline():
    """ Applies a pipeline of selecting features, applying transformations and an an SGD model. Parameters 
    and hyperparameters have been selected in a different program. """
    
    # Pipeline to apply the TfidfVectorizer (CountVectorizer+TfidfTransformer) on the processed column of the dataframe
    tfidf_features = Pipeline([
                        ('selector', TextSelector(key='processed')),
                        ('tfidf', TfidfVectorizer(max_features= 50000, max_df=0.75, ngram_range=(1, 3),
                                                  stop_words=stopwords.words('english')))
                    ])
    
    # Pipeline to apply a Counter on each of the parts of speech in the sentence (citation context), again applied on processed
    
    pos_features = Pipeline([
                        ('selector', TextSelector(key='processed')),
                        ('pos', PosTagMatrix(tokenizer=nltk.word_tokenize) ),
                    ])
    # Pipeline to apply a standard scaler on the numeric column num_positive_words
    positive_words_features =  Pipeline([
                                    ('selector', NumberSelector(key='num_positive_words')),
                                    ('standard', StandardScaler())
                                ])
    
    # Pipeline to apply a standard scaler on the numeric column num_negative_words
    negative_words_features = Pipeline([
                                    ('selector', NumberSelector(key='num_negative_words')),
                                    ('standard', StandardScaler())
                                ])
    
    features = FeatureUnion([
                        ('tfidf', tfidf_features),
                        ('pos', pos_features),
                        ('positive', positive_words_features),
                        ('negative', negative_words_features)
        
    ])
    #print(features.get_feature_names())
    feature_engineering = Pipeline([('features', features)])
    
    pipeline = Pipeline([
        ('features_set', feature_engineering),
        ('clf', SGDClassifier(alpha=0.0001, loss = 'hinge', max_iter=1000))
    ])

    return pipeline, feature_engineering
    
def create_train_test(X, y):
    """ Splits X and y (dataframe with sentences and Series with labels respectively) into training and test sets."""
    # There's no need of random_state any more when stratify (on the labels) is used, but it's included here anyway
    X_train, X_test, y_train, y_test = train_test_split(X, y, shuffle=True, stratify=y, random_state=13, test_size=0.2)
    #print("SHAPES of X_train={}, y_train={}, X_test={}, y_test={}".format(X_train.shape, y_train.shape, X_test.shape, y_test.shape))
    #print("VALUE COUNTS TRAIN AND TEST:")
    #print(y_train.value_counts())
    #print(y_test.value_counts())
    return X_train, X_test, y_train, y_test

def train_and_test(X_train, y_train, pipeline, feature_engineering, X_test, y_test):
    """ Func which applies training and test pipelines and returns a pd Series of predicted labels"""
    # Train/Fit
    pipeline.fit(X_train, y_train)
    #print(pipeline.steps)
    # Predict
    print(X_test.shape, X_train.shape)
    print(X_test.shape, X_train.shape)#, X_test)#, X_train.shape, X_test.shape)
    y_pred = pd.Series(pipeline.predict(X_test))#[['sentence', 'processed', 'num_negative_words', 'num_positive_words']]))
    return y_pred, pipeline

def calculate_holdoutset_metrics(y_train, y_test, y_pred, text_pipeline):
    """ Calculates a number of metrics on the holdout set after training and getting the predictions."""
    print("Predicted value counts per class (training set): ", y_train.value_counts())
    print(text_pipeline.steps)
    print("Predicted value counts per class (predictions): ", y_pred.value_counts())
    print("Predicted value counts per class (test set): ", y_test.value_counts())
    f1 = f1_score(y_test, y_pred, average=None)
    precision = precision_score(y_test, y_pred, average=None)
    recall = recall_score(y_test, y_pred, average=None)
    accuracy = accuracy_score(y_test, y_pred)
    print("F1={}, Precision={}, Recall={}, Accuracy={}".format(f1, precision, recall, accuracy))
    print(classification_report(y_test, y_pred, target_names=['negative', 'neutral', 'positive'] ))
    print("Confusion matrix: ", confusion_matrix(y_test, y_pred))
    
def main():
    """ Main function to train and test the model, and finally pickle it"""
    X, y = read_corpus_create_X_and_y()
    positive_polarity_words, negative_polarity_words = read_polar_phrases()
    X = processing(X, positive_polarity_words, negative_polarity_words)
    X_train, X_test, y_train, y_test = create_train_test(X, y)
    #print(X_train.shape, X_test.shape, y_train.shape, y_test.shape)
    pipeline, feature_engineering = create_pipeline()
    y_pred, pipeline = train_and_test(X_train, y_train, pipeline, feature_engineering, X_test, y_test)
    calculate_holdoutset_metrics(y_train, y_test, y_pred, pipeline)
    # Pickle the model using joblib
    joblib.dump(pipeline, 'citation_model_pipeline_v2.joblib')

if __name__ == '__main__':
    main()