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
process_count = None
# Block size in number of documents, generally takes 2.2MB/doc
block_size = 200
block_ext = '.blk'
temp_folder = 'tmp/'
content_key = 'content'
ngram_keys = ['unigram', 'bigram', 'trigram']

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

def get_block_folder_path(tag=''):
	script_path = os.path.dirname(os.path.realpath(__file__))
	block_folder = temp_folder
	block_folder += tag if tag == '' or tag.endswith('/') else tag + '/'
	return os.path.join(script_path, block_folder)

def get_block_path(tag, block_number):
	block_folder_path = get_block_folder_path(tag)
	if not os.path.exists(block_folder_path):
		os.makedirs(block_folder_path)
	return os.path.join(block_folder_path, str(block_number) + block_ext)

def deque_chunks(l, n):
	chunks = []
	"""Yield successive n-sized chunks from l."""
	for i in range(0, len(l), n):
		chunks.append(collections.deque(l[i:i + n]))
	return chunks

def process_block(file_paths, block_number):
	logging.info('Processing block #%s', block_number)
	block_index = {key:{} for key in ngram_keys}
	block_lengths = {key:{} for key in ngram_keys}
	i = 0
	while(len(file_paths)):
		file_path = file_paths.popleft()
		if not file_path.endswith('.xml'):
			continue
		logging.debug('[%s,%s] Extracting document', block_number, i)
		doc = utility.extract_doc(file_path)
		logging.debug('[%s,%s] Removing css text', block_number, i)
		doc[content_key] = utility.remove_css_text(doc[content_key])
		logging.debug('[%s,%s] Tokenizing document', block_number, i)
		doc[content_key] = utility.tokenize(doc[content_key])
		logging.debug('[%s,%s] Removing punctuations', block_number, i)
		doc[content_key] = utility.remove_punctuations(doc[content_key])
		logging.debug('[%s,%s] Removing stopwords', block_number, i)
		doc[content_key] = utility.remove_stopwords(doc[content_key])
		logging.debug('[%s,%s] Stemming tokens', block_number, i)
		doc[content_key] = utility.stem(doc[content_key])
		for k, ngram_key in enumerate(ngram_keys):
			n = k + 1
			logging.debug('[%s,%s] Generating %ss', block_number, i, ngram_key)
			doc[ngram_key] = utility.generate_ngrams(doc[content_key], n)
			logging.debug('[%s,%s] Counting %ss', block_number, i, ngram_key)
			doc[ngram_key] = utility.count_tokens(doc[ngram_key])
			logging.debug('[%s,%s] Processing %s postings and lengths', block_number, i, ngram_key)
			block_lengths[ngram_key][doc['document_id']] = get_length(doc[ngram_key])
			for term, freq in doc[ngram_key].items():
				if term not in block_index[ngram_key]:
					block_index[ngram_key][term] = []
				block_index[ngram_key][term].append((int(doc['document_id']), freq))
		i += 1

	logging.info('Saving block #%s', block_number)

	for ngram_key in ngram_keys:
		logging.debug('[%s] Saving %s block', block_number, ngram_key)
		# Save block
		block_index_path = get_block_path('_'.join(('index', ngram_key,)), block_number)
		block_lengths_path = get_block_path('_'.join(('lengths', ngram_key,)), block_number)

		with open(block_index_path, 'wb') as f:
			for term, postings_list in sorted(block_index[ngram_key].items()): # Each block sorted by term lexicographical order
				utility.save_object((term, postings_list,), f)

		with open(block_lengths_path, 'wb') as f:
			utility.save_object(block_lengths[ngram_key], f)

def usage():
	print("usage: " + sys.argv[0] + " -i directory-of-documents -d dictionary-file -p postings-file -l lengths-file")

def main():
	logging.info('Using block size of %s', block_size)
	logging.info('Peak memory consumption is estimated to be: {:,.2f}GB'.format(0.0022*block_size*multiprocessing.cpu_count()))
	dict_file = open(dict_path, 'wb')
	lengths_file = open(lengths_path, 'wb')
	postings_file = open(postings_path, 'wb')

	for dirpath, dirnames, filenames in os.walk(dir_doc):
		logging.info('Index size is estimated to be: {:,.1f}MB'.format(0.05*len(filenames)*5.1))
		filepaths = [os.path.join(dirpath, filename) for filename in sorted(filenames)] # Files read in order of DocID
		filepath_blocks = deque_chunks(filepaths, block_size)
		block_count = len(filepath_blocks)

		logging.info('Begin indexing')
		with multiprocessing.Pool(process_count) as pool:
			pool.starmap(process_block, zip(filepath_blocks, range(block_count)))

		# Merge step
		logging.info('Merging blocks')
		for ngram_key in ngram_keys:
			cumulative = 0
			logging.info('Merging %s block indexes', ngram_key)
			for dirpath, dirnames, filenames in os.walk(get_block_folder_path('_'.join(('index', ngram_key,)))):
				# Open all blocks concurrently in block number order
				filenames = sorted(filenames)
				block_file_handles = [open(os.path.join(dirpath, filename), 'rb') for filename in filenames if filename.endswith(block_ext)]
				term_postings_list_tuples = [utility.objects_in(block_file_handle) for block_file_handle in block_file_handles]
				# Merge blocks
				sorted_tuples = heapq.merge(*term_postings_list_tuples)

				logging.debug('Processing %s merge heap', ngram_key)
				target_term, target_postings_list = next(sorted_tuples)
				for term, postings_list in sorted_tuples:
					if target_term != term:
						doc_freq = len(target_postings_list)
						utility.save_object((target_term, doc_freq, cumulative), dict_file)
						cumulative += utility.save_object(target_postings_list, postings_file)
						target_term = term
						target_postings_list = postings_list
					else:
						target_postings_list.extend(postings_list)
				doc_freq = len(target_postings_list)
				utility.save_object((target_term, doc_freq, cumulative), dict_file)
				cumulative += utility.save_object(target_postings_list, postings_file)

				# Cleanup index file handles
				for block_file_handle in block_file_handles:
					block_file_handle.close()

			logging.info('Merging %s block lengths', ngram_key)
			lengths = {}
			for dirpath, dirnames, filenames in os.walk(get_block_folder_path('_'.join(('lengths', ngram_key,)))):
				filenames = sorted(filenames)
				for filename in filenames:
					if filename.endswith(block_ext):
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

	logging.info('[Multi-Process Single Pass In-Memory Indexer]')
	try:
		logging.debug('Deleting existing files')
		os.remove(dict_path)
		os.remove(postings_path)
		os.remove(lengths_path)
	except OSError:
		pass

	main()
