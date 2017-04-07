import getopt
import sys
import os
import utility
import math
import pickle
import logging
import collections
import multiprocessing
from time import time

max_block_size = 20 # Number of documents

log_every_n = 10
content_key = 'content'

def get_length(counted_tokens):
	sum_squares = 0
	for term, freq in counted_tokens.items():
		sum_squares += math.pow(1 + math.log10(freq), 2)
	return math.sqrt(sum_squares)

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

def get_folder_path(tag):
	script_path = os.path.dirname(os.path.realpath(__file__))
	temp_folder = 'tmp/'
	temp_folder += tag if tag.endswith('/') else tag + '/'
	return os.path.join(script_path, temp_folder)

def get_block_path(tag, block_number):
	temp_folder_path = get_folder_path(tag)
	if not os.path.exists(temp_folder_path):
		os.makedirs(temp_folder_path)
	return os.path.join(temp_folder_path, str(block_number))

def deque_chunks(l, n):
	chunks = []
	"""Yield successive n-sized chunks from l."""
	for i in range(0, len(l), n):
		chunks.append(collections.deque(l[i:i + n]))
	return chunks

def process_block(file_paths, block_number):
	logging.info('Processing block #%s', block_number)
	block_index = {}
	block_lengths = {}
	while(len(file_paths)):
		file_path = file_paths.popleft()
		if not file_path.endswith('.xml'):
			continue
		logging.debug('Extracting document')
		doc = utility.extract_doc(file_path)
		logging.debug('Tokenizing document')
		doc[content_key] = utility.tokenize(doc[content_key])
		logging.debug('Removing punctuations')
		doc[content_key] = utility.remove_punctuations(doc[content_key])
		logging.debug('Removing stopwords')
		doc[content_key] = utility.remove_stopwords(doc[content_key])
		logging.debug('Stemming tokens')
		doc[content_key] = utility.stem(doc[content_key])
		logging.debug('Generating ngrams')
		doc[content_key] = utility.generate_ngrams(doc[content_key], 1) # Unigram only
		logging.debug('Counting terms')
		doc[content_key] = utility.count_tokens(doc[content_key])
		logging.debug('Processing document')
		for term, freq in doc[content_key].items():
			if term not in block_index:
				block_index[term] = []
			block_index[term].append((doc['document_id'], freq))
			block_lengths[doc['document_id']] = get_length(doc[content_key])

	# Save block
	block_index_path = get_block_path('index', block_number)
	block_lengths_path = get_block_path('lengths', block_number)

	with open(block_index_path, 'wb') as f:
		for term, postings_list in sorted(block_index.items()): # Each block sorted by term lexicographical order
			utility.save_object((term, postings_list,), f)

	with open(block_lengths_path, 'wb') as f:
		utility.save_object(block_lengths, f)

def usage():
	print("usage: " + sys.argv[0] + " -i directory-of-documents -d dictionary-file -p postings-file -l lengths-file")

def main():
	for dirpath, dirnames, filenames in os.walk(dir_doc):
		filepaths = [os.path.join(dirpath, filename) for filename in sorted(filenames)] # Files read in order of DocID
		filepath_blocks = deque_chunks(filepaths, max_block_size)
		block_count = len(filepath_blocks)

		with multiprocessing.Pool() as pool:
			pool.starmap(process_block, zip(filepath_blocks, range(block_count)))

		# Merge step


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
