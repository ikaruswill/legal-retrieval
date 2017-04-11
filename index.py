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
BLOCK_SIZE = 200
BLOCK_EXT = '.blk'
TMP_PATH = 'tmp/'
CONTENT_KEY = 'content'
NGRAM_KEYS = ['unigram']
LENGTHS_PATH = 'lengths.txt'

def get_length(doc_index):
	sum_squares = 0
	for posting in doc_index.values():
		sum_squares += math.pow(1 + math.log10(posting[0]), 2)
	return math.sqrt(sum_squares)

def get_int_filename(filename):
	name = os.path.splitext(filename)[0]
	try:
		return int(name)
	except ValueError:
		return 0

def get_block_folder_path(tag=''):
	script_path = os.path.dirname(os.path.realpath(__file__))
	block_folder = TMP_PATH
	block_folder += tag if tag == '' or tag.endswith('/') else tag + '/'
	return os.path.join(script_path, block_folder)

def get_block_path(tag, block_number):
	block_folder_path = get_block_folder_path(tag)
	if not os.path.exists(block_folder_path):
		os.makedirs(block_folder_path)
	return os.path.join(block_folder_path, str(block_number) + BLOCK_EXT)

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
		doc_id = int(doc['document_id'])
		logging.debug('[%s,%s] Processing %s postings and lengths', block_number, i, CONTENT_KEY)
		doc_index = {}
		for j, term in enumerate(doc[CONTENT_KEY]):
			if term not in doc_index:
				doc_index[term] = [0, []]
			doc_index[term][0] += 1
			doc_index[term][1].append(j)
		i += 1
		sum_squares = 0
		for term, posting in doc_index.items():
			if term not in block_index:
				block_index[term] = []
			block_index[term].append((doc_id, posting[0], posting[1]))
			sum_squares += math.pow(1 + math.log10(posting[0]), 2)

		block_lengths[doc_id] = math.sqrt(sum_squares)

	logging.info('Saving block #%s', block_number)

	
	logging.debug('[%s] Saving %s block', block_number, CONTENT_KEY)
	# Save block
	block_index_path = get_block_path('_'.join(('index', CONTENT_KEY,)), block_number)
	block_lengths_path = get_block_path('_'.join(('lengths', CONTENT_KEY,)), block_number)

	with open(block_index_path, 'wb') as f:
		for term, postings_list in sorted(block_index.items()): # Each block sorted by term lexicographical order
			if postings_list == {}:
				print('WTF', term)
			utility.save_object((term, postings_list,), f)

	with open(block_lengths_path, 'wb') as f:
		utility.save_object(block_lengths, f)
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
		filepaths = [os.path.join(dirpath, filename) for filename in sorted(filenames, key=get_int_filename)] # Files read in order of DocID
		filepath_blocks = deque_chunks(filepaths, BLOCK_SIZE)
		block_count = len(filepath_blocks)

		logging.info('Begin indexing')
		with multiprocessing.Pool(PROCESS_COUNT) as pool:
			pool.starmap(process_block, zip(filepath_blocks, range(block_count)))

		# Merge step
		logging.info('Merging blocks')
		size = 0
		# for ngram_key in NGRAM_KEYS:
		logging.info('Merging %s block indexes', CONTENT_KEY)
		for dirpath, dirnames, filenames in os.walk(get_block_folder_path('_'.join(('index', CONTENT_KEY,)))):
			# Open all blocks concurrently in block number order
			filenames.sort(key=get_int_filename)
			block_file_handles = [open(os.path.join(dirpath, filename), 'rb') for filename in filenames if filename.endswith(BLOCK_EXT)]
			term_postings_list_tuples = [utility.objects_in(block_file_handle) for block_file_handle in block_file_handles]
			# Merge blocks
			sorted_tuples = heapq.merge(*term_postings_list_tuples)

			logging.debug('Processing %s merge heap', CONTENT_KEY)
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

			logging.info('Merging %s block lengths', CONTENT_KEY)
			lengths = {}
			for dirpath, dirnames, filenames in os.walk(get_block_folder_path('_'.join(('lengths', CONTENT_KEY,)))):
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

	main()
