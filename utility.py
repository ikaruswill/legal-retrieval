from string import punctuation
from nltk.tokenize import word_tokenize
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.stem.porter import PorterStemmer
from nltk.corpus import stopwords
from nltk.util import ngrams
from collections import Counter
import xml.etree.ElementTree
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

def str2bool(bool_str):
	return bool_str.lower() in ("yes", "true", "t", "1")

# Whitelist fields for better performance in both space and time complexity
def parse_child(child):
	if child.tag == 'str':
		return child.text
	elif child.tag == 'date':
		return child.text # Can do date parsing
	elif child.tag == 'bool':
		return str2bool(child.text)
	elif child.tag == 'long':
		return int(child.text) # Python 3 int does long implicitly
	elif child.tag == 'float':
		return float(child.text)
	elif child.tag == 'arr':
		arr = []
		for grandchild in child:
			arr.append(parse_child(grandchild))
		return arr
	else:
		exit('Unsupported tag: ', child.tag)

ignored_tag_names = set(['show', 'hide_url', 'hide_blurb', 'modified', 'date_modified', '_version_'])
def extract_doc(file_path):
	doc = {}
	root = xml.etree.ElementTree.parse(file_path).getroot()
	for child in root:
		key = child.attrib['name']
		if key not in ignored_tag_names:
			doc[key] = parse_child(child)

	return doc


class ScoreDocIDPair(object):
	def __init__(self, score, doc_id):
		self.score = score
		self.doc_id = doc_id

	def __lt__(self, other):
		return int(self.doc_id) < int(other.doc_id) if self.score == other.score else self.score < other.score

	def __repr__(self):
		return '%6s : %.10f' % (self.doc_id, self.score)

	def __str__(self):
		return '%6s : %.10f' % (self.doc_id, self.score)

