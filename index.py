import getopt
import sys
import os
import xml.etree.ElementTree
import utility
import math
import pickle

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
	iter_key_call(docs, key, utility.tokenize)
	iter_key_call(docs, key, utility.remove_punctuations)
	iter_key_call(docs, key, utility.remove_stopwords)
	iter_key_call(docs, key, utility.stem)

def build_dictionary(docs, key):
	terms = set()
	for doc in docs:
		terms.update(doc[key].keys())

	sorted_terms = sorted(list(terms))

	dictionary = {}
	for i, term in enumerate(sorted_terms):
		dictionary[term] = {'index': i}

	return dictionary

# def build_inverted_dictionary(dictionary):
# 	return [term for term, item in sorted(dictionary.items())]

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
def build_and_populate_postings(docs, key, dictionary):
	postings = []
	for term in dictionary:
		postings.append([])

	for doc in docs:
		doc_id = int(doc['document_id'])
		for term, freq in doc[key].items():
			index = dictionary[term]['index']
			# if len(postings[index]) > 0:
			# 	gap = doc_id - postings[index][-1][0]
			# 	postings[index].append((gap, freq))
			# else:
			# 	postings[index].append((doc_id, freq))
			postings[index].append((doc_id, freq))

	return postings

def save_postings(postings, f): 
	sizes = [] 
	serialized_postings = [] 

	cumulative = 0 
	for posting in postings: 
		serialized_posting = pickle.dumps(posting) 
		cumulative += len(serialized_posting) 
		sizes.append(cumulative) 
		serialized_postings.append(serialized_posting) 

	pickle.dump(sizes, f) 
	for serialized_posting in serialized_postings: 
		f.write(serialized_posting)

def iter_key_call(iterable, key, function, *args, **kwargs):
	for dict_item in iterable:
		dict_item[key] = function(dict_item[key], *args, **kwargs)

def copy_key(dicts, src_key, dest_key):
	for item in dicts:
		item[dest_key] = item[src_key]

def delete_key(dicts, delete_key):
	for item in dicts:
		item.pop(delete_key)

def usage():
	print("usage: " + sys.argv[0] + " -i directory-of-documents -d dictionary-file -p postings-file -l lengths-file")

def main():
	# Append binary mode for repeated pickling and creation of new file
	dict_file = open(dict_path, 'ab+')
	lengths_file = open(lengths_path, 'ab+')
	postings_file = open(postings_path, 'ab+')

	content_key = 'content'
	# ngram_keys = ['unigram', 'bigram', 'trigram']
	docs = load_xml_data(dir_doc)
	preprocess(docs, content_key)
	copy_key(docs, content_key, 'unigram')
	iter_key_call(docs, 'unigram', utility.count_tokens)
	lengths = build_and_populate_lengths(docs, 'unigram')
	dictionary = build_dictionary(docs, 'unigram')
	postings = build_and_populate_postings(docs, 'unigram', dictionary)
	save_postings(postings, postings_file)
	utility.save_object(dictionary, dict_file)
	utility.save_object(lengths, lengths_file)

	delete_key(docs, 'unigram')
	copy_key(docs, content_key, 'bigram')
	iter_key_call(docs, 'bigram', utility.generate_ngrams, n=2)
	iter_key_call(docs, 'bigram', utility.count_tokens)
	lengths = build_and_populate_lengths(docs, 'bigram')
	dictionary = build_dictionary(docs, 'bigram')
	postings = build_and_populate_postings(docs, 'bigram', dictionary)
	save_postings(postings, postings_file)
	utility.save_object(dictionary, dict_file)
	utility.save_object(lengths, lengths_file)

	delete_key(docs, 'bigram')
	copy_key(docs, content_key, 'trigram')
	delete_key(docs, content_key)
	iter_key_call(docs, 'trigram', utility.generate_ngrams, n=3)
	iter_key_call(docs, 'trigram', utility.count_tokens)
	lengths = build_and_populate_lengths(docs, 'trigram')
	dictionary = build_dictionary(docs, 'trigram')
	postings = build_and_populate_postings(docs, 'trigram', dictionary)
	save_postings(postings, postings_file)
	utility.save_object(dictionary, dict_file)
	utility.save_object(lengths, lengths_file)

	dict_file.close()
	lengths_file.close()
	postings_file.close()

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
