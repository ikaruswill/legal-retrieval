import getopt
import sys
import os
import utility
import math
import pickle
import logging
import collections

max_block_size = 20 # Number of tokens

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

def prepare_filepath(path, block_number):
	temp_folder = 'tmp/'
	path, name = os.path.split(path)
	name, ext = os.path.splitext(name)
	path = os.path.join(path, temp_folder)
	if not os.path.exists(path):
		os.makedirs(path)
	return os.path.join(path, name + '_' + str(block_number))

def usage():
	print("usage: " + sys.argv[0] + " -i directory-of-documents -d dictionary-file -p postings-file -l lengths-file")

def main():
	content_key = 'content'
	key = 'unigram'
	log_every_n = 10
	for dirpath, dirnames, filenames in os.walk(dir_doc):
		filename_queue = collections.deque(sorted(filenames)) # Sorted by DocID
		processed_count = 0
		iteration_count = 0
		block_size = 0
		while len(filename_queue):
			dictionary_set = set()
			postings_dict = {}
			lengths = {}
			while block_size < max_block_size and len(filename_queue):
				filename = filename_queue.popleft()
				if not filename.endswith('xml'):
					break
				file_path = os.path.join(dirpath, filename)
				doc = utility.extract_doc(file_path)
				logging.info('Tokenizing document')
				doc[content_key] = utility.tokenize(doc[content_key])
				logging.info('Removing punctuations')
				doc[content_key] = utility.remove_punctuations(doc[content_key])
				logging.info('Removing stopwords')
				doc[content_key] = utility.remove_stopwords(doc[content_key])
				logging.info('Stemming tokens')
				doc[content_key] = utility.stem(doc[content_key])
				logging.info('Generating ngrams')
				doc[content_key] = utility.generate_ngrams(doc[content_key], 1)
				logging.info('Counting terms')
				doc[content_key] = utility.count_tokens(doc[content_key])
				logging.info('Processing document')
				for term, freq in doc[content_key].items():
					if term not in dictionary_set:
						dictionary_set.add(term)
						postings_dict[term] = []
					postings_dict[term].append((doc['document_id'], freq))
					lengths[doc['document_id']] = get_length(doc[content_key])
				block_size += 1
				processed_count += 1

				if processed_count % log_every_n == 0:
					logging.info('-- Processed %s', processed_count)

			# Prepare dictionary and postings
			sorted_terms = sorted(list(dictionary_set))
			dictionary = {}
			postings = []
			for i, term in enumerate(sorted_terms):
				dictionary[term] = {'index': i, 'doc_freq': len(postings_dict[term])}
				postings.append(postings_dict[term]) # Append list reference

			# Save block
			# NEED TO HANDLE RELATIVE PATHS AND LACK OF TRAILING SLASHES
			block_dict_path = prepare_filepath(dict_path, iteration_count)
			block_lengths_path = prepare_filepath(lengths_path, iteration_count)
			block_postings_path = prepare_filepath(postings_path, iteration_count)

			dict_file = open(block_dict_path, 'wb')
			lengths_file = open(block_lengths_path, 'wb')
			postings_file = open(block_postings_path, 'wb')

			utility.save_object(dictionary, dict_file)
			utility.save_object(lengths, lengths_file)
			utility.save_object(postings_dict, postings_file)

			dict_file.close()
			lengths_file.close()
			postings_file.close()

			block_size = 0
			iteration_count += 1
			print('--- Iteration Count: ', iteration_count)

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
