from string import punctuation
from nltk.tokenize import word_tokenize
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.stem.porter import PorterStemmer
from nltk.corpus import stopwords
from nltk.util import ngrams
from collections import Counter
import pickle

def tokenize(string):
		return word_tokenize(string.lower())

def remove_punctuations(tokens):
	punctuation_set = set(punctuation)
	return [token for token in tokens if token not in punctuation_set]

def remove_stopwords(tokens):
	stopword_set = set(stopwords.words('english'))
	return [token for token in tokens if token not in stopword_set]

def lemmatize(tokens):
	wnl = WordNetLemmatizer()
	return [wnl.lemmatize(token) for token in tokens]

def stem(tokens):
	stemmer = PorterStemmer()
	return [stemmer.stem(token) for token in tokens]

def generate_ngrams(tokens, n, pad=False, start_sym='<s>', end_sym='</s>'):
	return [' '.join(ngram) for ngram in ngrams(tokens, n, pad_left=pad, pad_right=pad, left_pad_symbol=start_sym, right_pad_symbol=end_sym)]

def count_tokens(tokens):
	return Counter(tokens)

def save_object(obj, f):
	pickle.dump(obj, f)

def load_object(f):
	return pickle.load(f)