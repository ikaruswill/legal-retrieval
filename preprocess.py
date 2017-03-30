from string import punctuation
from nltk.tokenize import word_tokenize
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.corpus.stopwords import stopwords
from nltk.util import ngrams
from collections import Counter

# In-place
def tokenize(docs, key):
	for doc_id, doc in docs.items():
		docs[doc_id][key] = word_tokenize(doc[key].lower())

# In-place, requires tokenized
def remove_punctuations(docs, key):
	punctuation_set = set(string.punctuation)
	for doc_id, doc in docs.items():
		docs[doc_id][key] = [token for token in doc[key] if token not in punctuation_set]

# In-place, requires tokenized
def remove_stopwords(docs, key):
	stopword_set = set(stopwords.words('english'))
	for doc_id, doc in docs.items():
		docs[doc_id][key] = [token for token in doc[key] if token not in stopword_set]

# In-place, requires tokenized, stopwords removed.
def lemmatize(docs, key):
	wnl = WordNetLemmatizer()
	for doc_id, doc in docs.items():
		docs[doc_id][key] = [wnl.lemmatize(token) for token in doc[key]]

# In-place
def generate_ngrams(docs, key, n, pad, start_sym='<s>', end_sym='</s>'):
	for doc_id, doc in docs.items():
		docs[doc_id][key] = [' '.join(ngram) for ngram in ngrams(doc[key], n, pad_left=pad, pad_right=pad, left_pad_symbol=start_sym, right_pad_symbol=end_sym)]

# In-place, destructive
def count_tokens(docs, key):
	for doc_id, doc in docs.items():
		docs[doc_id][key] = Counter(doc[key])
