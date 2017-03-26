from string import punctuation
from nltk.tokenize import word_tokenize
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.corpus.stopwords import stopwords

# In-place
def tokenize(docs, key):
	for doc_id, doc in docs.items():
		docs[doc_id][key] = word_tokenize(doc[key].lower())

# In-place
def remove_punctuations(docs, key):
	punctuation_set = set(string.punctuation)
	for doc_id, doc in docs.items():
		docs[doc_id][key] = [token for token in doc[key] if token not in punctuation_set]

# In-place
def remove_stopwords(docs, key):
	stopword_set = set(stopwords.words('english'))
	for doc_id, doc in docs.items():
		docs[doc_id][key] = [token for token in doc[key] if token not in stopword_set]

# In-place
def lemmatize(docs, key):
	wnl = WordNetLemmatizer()
	for doc_id, doc in docs.items():
		docs[doc_id][key] = [wnl.lemmatize(token) for token in doc[key]]
