from string import punctuation
from nltk.tokenize import word_tokenize
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.corpus import stopwords
from nltk.util import ngrams
from collections import Counter
import pickle

# In-place
def tokenize(docs, key):
	for doc in docs:
		doc[key] = word_tokenize(doc[key].lower())

# In-place, requires tokenized
def remove_punctuations(docs, key):
	punctuation_set = set(punctuation)
	for doc in docs:
		doc[key] = [token for token in doc[key] if token not in punctuation_set]

# In-place, requires tokenized
def remove_stopwords(docs, key):
	stopword_set = set(stopwords.words('english'))
	for doc in docs:
		doc[key] = [token for token in doc[key] if token not in stopword_set]

# In-place, requires tokenized, stopwords removed.
def lemmatize(docs, key):
	wnl = WordNetLemmatizer()
	for doc in docs:
		doc[key] = [wnl.lemmatize(token) for token in doc[key]]

# In-place
def generate_ngrams(docs, key, n, pad=False, start_sym='<s>', end_sym='</s>'):
	for doc in docs:
		doc[key] = [' '.join(ngram) for ngram in ngrams(doc[key], n, pad_left=pad, pad_right=pad, left_pad_symbol=start_sym, right_pad_symbol=end_sym)]

# In-place, destructive
def count_tokens(docs, key):
	for doc in docs:
		doc[key] = Counter(doc[key])

def save_object(object, path):
	with open(path, 'ab+') as f:
		pickle.dump(object)