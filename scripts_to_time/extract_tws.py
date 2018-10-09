import spacy
import pandas as pd
import sys

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
df = pd.read_csv('./data/winemag-data-130k-v2.csv', index_col=0)

df_sample = df.sample(n=int(sys.argv[1]), replace=False)

# extract topic words, save
df_sample.description.apply(extract_lemmatized_topic_words)
df_sample.to_csv('./data/sample_topic_words.csv', index=False)
