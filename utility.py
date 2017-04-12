from string import punctuation
from nltk.tokenize import word_tokenize
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.stem.porter import PorterStemmer
from nltk.corpus import stopwords
from nltk.util import ngrams
from collections import Counter
import xml.etree.ElementTree
import pickle
import json
import re

# Config persistence path
config_path = 'config.tmp'

# Config persistence functions
def save_config(args):
	with open(config_path, 'w') as f:
		json.dump(args, f)

def load_config():
	with open(config_path, 'r') as f:
		return json.load(f)

# Document extraction variables
ignored_tag_names = set(['show', 'hide_url', 'hide_blurb', 'modified', 'date_modified', '_version_'])

# Document extraction functions
def str2bool(bool_str):
	return bool_str.lower() in ("yes", "true", "t", "1")

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

def extract_doc(file_path):
	doc = {}
	root = xml.etree.ElementTree.parse(file_path).getroot()
	for child in root:
		key = child.attrib['name']
		if key not in ignored_tag_names:
			doc[key] = parse_child(child)

	return doc

# Preprocessing variables
stopword_set = set(stopwords.words('english'))
punctuation_set = set(punctuation)
wnl = WordNetLemmatizer()
stemmer = PorterStemmer()

# Preprocessing functions, in order of application
def tokenize(string):
	return word_tokenize(string.lower())

def remove_css_text(string):
	return re.sub('[\.|#|@][\w\.\-]+[ \t]*[\w\.\-]+{.+} *$', '', string, flags=re.DOTALL|re.MULTILINE)

def remove_punctuations(tokens):
	return [token for token in tokens if token not in punctuation_set]

def remove_stopwords(tokens):
	return [token for token in tokens if token not in stopword_set]

def lemmatize(tokens):
	return [wnl.lemmatize(token) for token in tokens]

def stem(tokens):
	return [stemmer.stem(token) for token in tokens]

def generate_ngrams(tokens, n, pad=False, start_sym='<s>', end_sym='</s>'):
	if n == 1:
		return tokens
	return [' '.join(ngram) for ngram in ngrams(tokens, n, pad_left=pad, pad_right=pad, left_pad_symbol=start_sym, right_pad_symbol=end_sym)]

def count_tokens(tokens):
	return Counter(tokens)


# Object handling functions
def save_object(obj, f):
	s_obj = pickle.dumps(obj)
	f.write(s_obj)
	return len(s_obj)

def load_object(f):
	return pickle.load(f)

def objects_in(f):
	while True:
		try:
			yield pickle.load(f)
		except EOFError:
			return

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