import getopt
import sys
import os
import xml.etree.ElementTree
import utility
import math
import json

ignored_tag_names = set(['show', 'hide_url', 'hide_blurb', 'modified', 'date_modified', '_version_'])

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
			arr.append(parse_child)
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
	
def load_xml_data(dir_doc):
	docs = []
	for dirpath, dirnames, filenames in os.walk(dir_doc):
		for name in sorted(filenames):
			if name.endswith('.xml'):
				file_path = os.path.join(dirpath, name)
				docs.append(extract_doc(file_path))

	return docs

def preprocess(docs, key):
	utility.tokenize(docs, key)
	utility.remove_punctuations(docs, key)
	utility.remove_stopwords(docs, key)
	utility.lemmatize(docs, key)

def build_dictionary(docs, key):
	terms = set()
	for doc in docs:
		terms.update(doc[key].keys())

	sorted_terms = sorted(list(terms))

	dictionary = {}
	for i, term in enumerate(sorted_terms):
		dictionary[term] = {'index': i}

	return dictionary

def build_inverted_dictionary(dictionary):
	return [term for term, item in sorted(dictionary.items())]


def build_and_populate_lengths(docs, key):
	lengths = {}
	for doc in docs:
		sum_squares = 0
		for term, freq in doc[key].items():
			sum_squares += math.pow(1 + math.log10(freq), 2)
		doc_id = doc['document_id']
		lengths[doc_id] = math.sqrt(sum_squares)

	return lengths

# Also modifies dictionary by adding doc_freq key for each term
def generate_and_save_postings(docs, key, dictionary, postings_path):
	tempfile_path = 'index.tmp'
	with open(postings_path, 'ab+') as postings_handle,\
	open(tempfile_path, 'wb+') as tempfile_handle:
		seek_table = []
		cumulative = 0
		for term, item in dictionary.items():
			term_postings = get_term_postings(docs, key, term)
			item['doc_freq'] = len(term_postings)
			cumulative += write_term_postings(term_postings, tempfile_handle)
			seek_table.append(cumulative)
		save_seek_table(seek_table, postings_handle, tempfile_handle)
	os.remove(tempfile_path)

def get_term_postings(docs, key, term):
	term_postings = []
	for doc in docs:
		if term in doc[key]:
			doc_id = doc['document_id']
			freq = doc[key][term]
			if len(term_postings) == 0:
				term_postings.append((doc_id, freq))
			else:
				gap = str(int(doc_id) - int(term_postings[-1][0]))
				term_postings.append((gap, freq))

	return term_postings

def copy_key(dicts, src_key, dest_key):
	for item in dicts:
		item[dest_key] = item[src_key]

def delete_key(dicts, delete_key):
	for item in dicts:
		item.pop(delete_key)

# File must be opened in binary mode
def write_term_postings(term_postings, file_handle):
	serialized = json.dumps(term_postings)
	file_handle.write(serialized)
	return len(serialized)

# File must be opened in binary mode
def save_seek_table(seek_table, postings_handle, tempfile_handle):
	json.dump(seek_table, postings_handle)
	postings_handle.write(tempfile_handle.read())

def usage():
	print("usage: " + sys.argv[0] + " -i directory-of-documents -d dictionary-file -p postings-file -l lengths-file")

def main():
	content_key = 'content'
	# ngram_keys = ['unigram', 'bigram', 'trigram']
	docs = load_xml_data(dir_doc)
	preprocess(docs, content_key)
	copy_key(docs, content_key, 'unigram')
	utility.count_tokens(docs, 'unigram')
	lengths = build_and_populate_lengths(docs, 'unigram')
	dictionary = build_dictionary(docs, 'unigram')
	generate_and_save_postings(docs, 'unigram', dictionary, postings_path)
	utility.save_object(dictionary, dict_path)
	utility.save_object(lengths, lengths_path)

	delete_key(docs, 'unigram')
	copy_key(docs, content_key, 'bigram')
	utility.generate_ngrams(docs, 'bigram', 2, False)
	utility.count_tokens(docs, 'bigram')
	lengths = build_and_populate_lengths(docs, 'bigram')
	dictionary = build_dictionary(docs, 'bigram')
	generate_and_save_postings(docs, 'bigram', dictionary, postings_path)
	utility.save_object(dictionary, dict_path)
	utility.save_object(lengths, lengths_path)

	delete_key(docs, 'bigram')
	copy_key(docs, content_key, 'trigram')
	delete_key(docs, content_key)
	utility.generate_ngrams(docs, 'trigram', 3, False)
	utility.count_tokens(docs, 'trigram')
	lengths = build_and_populate_lengths(docs, 'trigram')
	dictionary = build_dictionary(docs, 'trigram')
	generate_and_save_postings(docs, 'trigram', dictionary, postings_path)
	utility.save_object(dictionary, dict_path)
	utility.save_object(lengths, lengths_path)

if __name__ == '__main__':
	dir_doc = dict_path = postings_path = lengths_path = None
	try:
		opts, args = getopt.getopt(sys.argv[1:], 'i:d:p:l:')
	except getopt.GetoptError as err:
		usage()
		sys.exit(2)
	for o, a in opts:
		if o == '-i':
			dir_doc = a
		elif o == '-d':
			dict_path = a
		elif o == '-p':
			postings_path = a
		elif o == '-l':
			lengths_path = a
		else:
			assert False, "unhandled option"
	if dir_doc == None or dict_path == None or postings_path == None or lengths_path == None:
		usage()
		sys.exit(2)

	try:
		os.remove(dict_path)
		os.remove(postings_path)
		os.remove(lengths_path)
	except OSError:
		pass

	main()
