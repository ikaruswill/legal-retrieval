import collections
import getopt
import heapq
import logging
import math
import multiprocessing
import os
import pickle
import shutil
import sys
import utility

# Set none for max processes
PROCESS_COUNT = None
# Block size in number of documents, generally takes 1.25MB/doc
BLOCK_SIZE = 400
BLOCK_EXT = '.blk'
TMP_PATH = 'tmp/'
CONTENT_KEY = 'content'
NGRAM_KEYS = ['unigram', 'bigram']
FILE_BLACKLIST = set(['3074605.xml', '3074613.xml'])
LENGTHS_PATH = 'lengths.txt'

def get_length(counted_tokens):
	"""
	Calculate the Euclidean norm of the document vector formed from the Bag-of-words

	Args:
		counted_tokens: dict of term:frequency items
	
	Returns:
		The Euclidean norm of the document vector
	"""
	sum_squares = 0
	for term, freq in counted_tokens.items():
		sum_squares += math.pow(1 + math.log10(freq), 2)
	return math.sqrt(sum_squares)

def get_int_filename(filename):
	""" Get the integer value of a filename """
	name = os.path.splitext(filename)[0]
	try:
		return int(name)
	except ValueError:
		return 0

def get_block_folder_path(tag=''):
	"""
	Get the absolute block folder path that is associated to the specified tag

	Args:
		tag: The tag used to identify the type of data stored in the block
	
	Returns:
		The absolute path to the block folder
	"""
	script_path = os.path.dirname(os.path.realpath(__file__))
	block_folder = TMP_PATH
	block_folder += tag if tag == '' or tag.endswith('/') else tag + '/'
	return os.path.join(script_path, block_folder)

def get_block_path(tag, block_number):
	"""
	Get the absolute path of a specific block identified by a tag and its unique identifier

	Args:
		tag: The tag used to identify the type of data stored in the block
		block_number: The unique identifier of the block
	
	Returns:
		The absolute path to the block
	"""
	block_folder_path = get_block_folder_path(tag)
	if not os.path.exists(block_folder_path):
		os.makedirs(block_folder_path)
	return os.path.join(block_folder_path, str(block_number) + BLOCK_EXT)

def deque_chunks(l, n):
	chunks = []
	""" Yield successive n-sized chunks from l. """
	for i in range(0, len(l), n):
		chunks.append(collections.deque(l[i:i + n]))
	return chunks

def process_block(file_paths, block_number):
	"""
	Preprocess a block defined by a number of file paths and a unique block identifier
	and save them term-at-a-time to a temporary block file.

	Args:
		file_paths: List of document file paths assigned to the block
		block_number: Unique identifier for the block
	"""
	logging.info('Processing block #%s', block_number)
	block_index = {key:{} for key in NGRAM_KEYS}
	block_lengths = {key:{} for key in NGRAM_KEYS}
	i = 0
	while(len(file_paths)):
		file_path = file_paths.popleft()
		if not file_path.endswith('.xml'):
			continue
		logging.debug('[%s,%s] Extracting document %s', block_number, i, os.path.split(file_path)[-1])
		doc = utility.extract_doc(file_path)
		logging.debug('[%s,%s] Removing CSS elements', block_number, i)
		doc[CONTENT_KEY] = utility.remove_css_text(doc[CONTENT_KEY])
		logging.debug('[%s,%s] Tokenizing document', block_number, i)
		doc[CONTENT_KEY] = utility.tokenize(doc[CONTENT_KEY])
		logging.debug('[%s,%s] Removing punctuations', block_number, i)
		doc[CONTENT_KEY] = utility.remove_punctuations(doc[CONTENT_KEY])
		logging.debug('[%s,%s] Removing stopwords', block_number, i)
		doc[CONTENT_KEY] = utility.remove_stopwords(doc[CONTENT_KEY])
		logging.debug('[%s,%s] Stemming tokens', block_number, i)
		doc[CONTENT_KEY] = utility.stem(doc[CONTENT_KEY])
		for k, ngram_key in enumerate(NGRAM_KEYS):
			n = k + 1
			doc_id = int(doc['document_id'])
			logging.debug('[%s,%s] Generating %ss', block_number, i, ngram_key)
			doc[ngram_key] = utility.generate_ngrams(doc[CONTENT_KEY], n)
			logging.debug('[%s,%s] Counting %ss', block_number, i, ngram_key)
			doc[ngram_key] = utility.count_tokens(doc[ngram_key])
			logging.debug('[%s,%s] Processing %s postings and lengths', block_number, i, ngram_key)
			block_lengths[ngram_key][doc_id] = get_length(doc[ngram_key])
			for term, freq in doc[ngram_key].items():
				if term not in block_index[ngram_key]:
					block_index[ngram_key][term] = []
				block_index[ngram_key][term].append((doc_id, freq,))
		i += 1

	logging.info('Saving block #%s', block_number)

	for ngram_key in NGRAM_KEYS:
		logging.debug('[%s] Saving %s block', block_number, ngram_key)
		# Save block
		block_index_path = get_block_path('_'.join(('index', ngram_key,)), block_number)
		block_lengths_path = get_block_path('_'.join(('lengths', ngram_key,)), block_number)

		with open(block_index_path, 'wb') as f:
			for term, postings_list in sorted(block_index[ngram_key].items()): # Each block sorted by term lexicographical order
				utility.save_object((term, postings_list,), f)

		with open(block_lengths_path, 'wb') as f:
			utility.save_object(block_lengths[ngram_key], f)
	logging.info('Block #%s complete', block_number)

def usage():
	print("usage: " + sys.argv[0] + " -i directory-of-documents -d dictionary-file -p postings-file -l lengths-file")

def main():
	logging.info('[Multi-Process Single Pass In-Memory Indexer]')
	try:
		logging.debug('Deleting existing files')
		os.remove(dict_path)
		os.remove(postings_path)
		os.remove(LENGTHS_PATH)
	except OSError:
		pass

	logging.info('Using block size of %s', BLOCK_SIZE)
	logging.info('Peak memory consumption is estimated to be: {:,.2f}GB'.format(0.00125*BLOCK_SIZE*multiprocessing.cpu_count()))
	dict_file = open(dict_path, 'wb')
	lengths_file = open(LENGTHS_PATH, 'wb')
	postings_file = open(postings_path, 'wb')

	for dirpath, dirnames, filenames in os.walk(dir_doc):
		logging.info('Collection cardinality is: {:,}'.format(len(filenames)))
		logging.info('Index size is estimated to be: {:,.1f}MB'.format(0.089*len(filenames)))
		logging.info('Models set: {!r}'.format(NGRAM_KEYS))
		# Files read in order of DocID
		filepaths = [os.path.join(dirpath, filename) for filename in sorted(filenames, key=get_int_filename) if filename not in FILE_BLACKLIST]
		# Divide files into blocks
		filepath_blocks = deque_chunks(filepaths, BLOCK_SIZE)
		block_count = len(filepath_blocks)

		logging.info('Begin indexing')
		with multiprocessing.Pool(PROCESS_COUNT) as pool:
			pool.starmap(process_block, zip(filepath_blocks, range(block_count)))

	# Block merging step
	logging.info('Merging blocks')
	size = 0
	for ngram_key in NGRAM_KEYS:
		logging.info('Merging %s block indexes', ngram_key)
		for dirpath, dirnames, filenames in os.walk(get_block_folder_path('_'.join(('index', ngram_key,)))):
			# Open all blocks concurrently in block number order
			filenames.sort(key=get_int_filename)
			block_file_handles = [open(os.path.join(dirpath, filename), 'rb') for filename in filenames if filename.endswith(BLOCK_EXT)]
			term_postings_list_tuples = [utility.objects_in(block_file_handle) for block_file_handle in block_file_handles]
			# Merge blocks with lazy loading
			sorted_tuples = heapq.merge(*term_postings_list_tuples)

			logging.debug('Processing %s merge heap', ngram_key)
			# Buffer first term, postings pair in memory
			target_term, target_postings_list = next(sorted_tuples)
			for term, postings_list in sorted_tuples:
				# Save current pair to file if next term in lexicographical order is different
				# Also buffer next pair for future comparison cycles
				if target_term != term:
					utility.save_object((target_term, size), dict_file)
					size = utility.save_object(target_postings_list, postings_file)
					target_term = term
					target_postings_list = postings_list
				else:
				# Merge duplicate pairs from heap, in memory buffer
					target_postings_list.extend(postings_list)
			# Save last pair buffered in memory as no subsequent pairs exist 
			utility.save_object((target_term, size), dict_file)
			size = utility.save_object(target_postings_list, postings_file)
			# Save a marker in dictionary between models
			utility.save_object((None, None), dict_file)

			# Cleanup index file handles
			for block_file_handle in block_file_handles:
				block_file_handle.close()

		logging.info('Merging %s block lengths', ngram_key)
		lengths = {}
		for dirpath, dirnames, filenames in os.walk(get_block_folder_path('_'.join(('lengths', ngram_key,)))):
			filenames.sort(key=get_int_filename)
			for filename in filenames:
				if filename.endswith(BLOCK_EXT):
					with open(os.path.join(dirpath, filename), 'rb') as f:
						lengths.update(utility.load_object(f))
			utility.save_object(lengths, lengths_file)

	logging.info('Cleaning up blocks')
	# Cleanup block files
	shutil.rmtree(get_block_folder_path())

	dict_file.close()
	lengths_file.close()
	postings_file.close()
	logging.info('Indexing complete')

if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO, datefmt='%d/%m %H:%M:%S', format='%(asctime)s %(message)s')
	dir_doc = dict_path = postings_path = None
	try:
		opts, args = getopt.getopt(sys.argv[1:], 'i:d:p:')
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
		else:
			assert False, "unhandled option"
	if dir_doc == None or dict_path == None or postings_path == None:
		usage()
		sys.exit(2)

	dir_doc += '/' if not dir_doc.endswith('/') else ''

	utility.save_config({'dir_doc': dir_doc, 'dict_path': dict_path, 'postings_path': postings_path, 'lengths_path': LENGTHS_PATH})

	main()
