import string
from nltk.tokenize import word_tokenize
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.corpus import stopwords

# In-place
def tokenize(docs, key):
	for doc_id, doc in docs.items():
		docs[doc_id][key] = word_tokenize(doc[key].lower())

# In-place
def remove_punctuations(docs, key):
	punctuation = set(string.punctuation)
	for doc_id, doc in docs.items():
		docs[doc_id][key] = [token for token in doc[key] if token not in punctuation]

# In-place
def lemmatize(docs, key):
	wnl = WordNetLemmatizer()
	for doc_id, doc in docs.items():
		docs[doc_id][key] = [wnl.lemmatize(token) for token in doc[key]]
