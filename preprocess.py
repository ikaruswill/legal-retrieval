import string
from nltk.tokenize import word_tokenize
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.corpus import stopwords

# In-place
def tokenize(docs, key):
	for doc_id, doc in docs.items():
		docs[doc_id][key] = word_tokenize(doc[key].lower())

