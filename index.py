import getopt
import sys
import os
import xml.etree.ElementTree
import utility

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
	docs = {}
	for dirpath, dirnames, filenames in os.walk(dir_doc):
		for name in filenames:
			if name.endswith('.xml'):
				file_path = os.path.join(dirpath, name)
				doc_id = os.path.splitext(name)[0]
				docs[name] = extract_doc(file_path)

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

def build_and_populate_lengths(docs, key):
	lengths = {}
	for doc_id, doc in docs.items():
		sum_squares = 0
		for term, freq in doc[key].items():
			sum_squares += math.pow(1 + math.log10(freq), 2)
		lengths[doc_id] = math.sqrt(sum_squares)

	return lengths

def build_and_populate_postings(docs, dictionary, key):
	postings = []
	for term in dictionary:
		postings.append([])

	for doc_id, doc in sorted(docs.items()):
		for term, freq in doc[key]:
			index = dictionary[term]
			postings[index].append((doc_id, freq))

	return postings

def populate_dictionary(dictionary, postings):
	for term, dict_item in dictionary.items():
		dictionary[term]['doc_freq'] = len(postings[dict_item['index']])

def copy_key(dict_of_dicts, src_key, dest_key):
	for key, item in dict_of_dicts.items():
		dict_of_dicts[key][dest_key] = item[src_key]

def delete_key(dict_of_dicts, delete_key):
	for key, item in dict_of_dicts.items():
		dict_of_dicts[key].pop(delete_key)

def save_postings(postings, postings_path):
	sizes = []
	pickled_postings = []

	cumulative = 0
	for posting in postings:
		pickled_posting = pickle.dumps(posting)
		cumulative += len(pickled_posting)
		sizes.append(cumulative)
		pickled_postings.append(pickled_posting)

	with open(postings_path, 'ab+') as f:
		pickle.dump(sizes, f)
		for pickled_posting in pickled_postings:
			f.write(pickled_posting)

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
	postings = build_and_populate_postings(docs, dictionary, 'unigram')
	populate_dictionary(dictionary, postings)
	utility.save_object(dictionary, dict_path)
	utility.save_object(lengths, lengths_path)
	save_postings(postings, postings_path)

	delete_key(docs, 'unigram')
	copy_key(docs, content_key, 'bigram')
	utility.generate_ngrams(docs, 'bigram', 2, False)
	utility.count_tokens(docs, 'bigram')
	lengths = build_and_populate_lengths(docs, 'bigram')
	dictionary = build_dictionary(docs, 'bigram')
	postings = build_and_populate_postings(docs, dictionary, 'bigram')
	populate_dictionary(dictionary, postings)
	utility.save_object(dictionary, dict_path)
	utility.save_object(lengths, lengths_path)
	save_postings(postings, postings_path)

	delete_key(docs, 'bigram')
	copy_key(docs, content_key, 'trigram')
	utility.generate_ngrams(docs, 'trigram', 3, False)
	utility.count_tokens(docs, 'trigram')
	lengths = build_and_populate_lengths(docs, 'trigram')
	dictionary = build_dictionary(docs, 'trigram')
	postings = build_and_populate_postings(docs, dictionary, 'trigram')
	populate_dictionary(dictionary, postings)
	utility.save_object(dictionary, dict_path)
	utility.save_object(lengths, lengths_path)
	save_postings(postings, postings_path)

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

	main()
