import spacy
import sys

import dask.dataframe as dd
import pandas as pd

from multiprocessing import cpu_count

def is_valid(tk):
    """ Return True if token is not a stopword, punctuation character, blank space or digit."""
    invalid_conditions = (
        tk.is_stop,
        tk.pos_ == 'PUNCT',
        tk.lemma_ == ' ',
        tk.lemma_.isdigit()
    )
    if any(invalid_conditions):
        return False
    return True

def extract_lemmatized_topic_words(doc):
    return [tk.lemma_ for tk in nlp(doc) if is_valid(tk)]

# load spacy
nlp = spacy.load('en')

# load data
ddf = dd.read_csv('./data/winemag-data-130k-v2.csv')
ddf = ddf.drop('Unnamed: 0', axis=1)

# get sample
frac = int(sys.argv[1]) / len(ddf)
ddf_sample = ddf.sample(frac=frac, replace=False)

# use all available cores
cores = cpu_count()
ddf_sample = ddf_sample.repartition(npartitions=cores)

# add tw
ddf_sample['topic_words'] = ddf_sample.description.apply(extract_lemmatized_topic_words,
                                                         args=(),
                                                         meta=pd.Series(dtype='object', name='topic_words'))
