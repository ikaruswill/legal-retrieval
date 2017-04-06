import getopt
import sys
import os
import utility
import math
import pickle
import logging
import multiprocessing
import itertools

def load_xml_data(dir_doc):
	docs = []
	for dirpath, dirnames, filenames in os.walk(dir_doc):
		for name in sorted(filenames):
			if name.endswith('.xml'):
				file_path = os.path.join(dirpath, name)
				docs.append(utility.extract_doc(file_path))

	return docs

def preprocess(doc, key):
	doc[key] = utility.tokenize(doc[key])
	doc[key] = utility.remove_punctuations(doc[key])
	doc[key] = utility.remove_stopwords(doc[key])
	doc[key] = utility.stem(doc[key])
	return doc

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
	logging.info('Creating new files')
	# Append binary mode for repeated pickling and creation of new file
	dict_file = open(dict_path, 'ab+')
	lengths_file = open(lengths_path, 'ab+')
	postings_file = open(postings_path, 'ab+')

	logging.info('Parsing XML')
	content_key = 'content'
	docs = load_xml_data(dir_doc)
	logging.info('Preprocessing documents')
	with multiprocessing.Pool(3) as pool:
		docs = pool.starmap(preprocess, zip(docs, itertools.repeat(content_key)))

	keys = ['unigram', 'bigram', 'trigram']

	for i, key in enumerate(keys):
		ngram = i + 1
		logging.info('[%s] Copying key', key)
		copy_key(docs, content_key, key)
		logging.info('[%s] Generating ngrams', key)
		iter_key_call(docs, key, utility.generate_ngrams, n=ngram)
		logging.info('[%s] Counting tokens', key)
		iter_key_call(docs, key, utility.count_tokens)
		logging.info('[%s] Building & populating lengths', key)
		lengths = build_and_populate_lengths(docs, key)
		logging.info('[%s] Building dictionary', key)
		dictionary = build_dictionary(docs, key)
		logging.info('[%s] Building & populating postings', key)
		postings = build_and_populate_postings(docs, key, dictionary)
		logging.info('[%s] Saving postings', key)
		save_postings(postings, postings_file)
		logging.info('[%s] Saving dictionary and lengths', key)
		utility.save_object(dictionary, dict_file)
		utility.save_object(lengths, lengths_file)
		delete_key(docs, key)

	logging.info('Closing files')
	dict_file.close()
	lengths_file.close()
	postings_file.close()
	logging.info('Indexing complete')

if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO, datefmt='%d/%m/%y %H:%M:%S', format='%(asctime)s %(message)s')
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

	logging.info('Begin indexing')
	try:
		logging.info('Deleting existing files')
		os.remove(dict_path)
		os.remove(postings_path)
		os.remove(lengths_path)
	except OSError:
		pass

	main()
