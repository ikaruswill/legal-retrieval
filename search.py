import getopt
import sys
import utility
from utility import ScoreDocIDPair
import math
import heapq
import os

unigram_dict = {}
bigram_dict = {}
unigram_lengths = {}
bigram_lengths = {}
unigram_postings_offset = {}
bigram_postings_offset = {}
postings_file = {}
unigram_start_offset = 0
bigram_start_offset = 0
doc_query_cache = {}

POST_PROCESSOR_DIR = './query_exp_results.txt'


def get_posting(index, start_offset, postings_offsets):
	byte_offset = start_offset + postings_offsets[index]
	postings_file.seek(byte_offset, 0)
	posting = utility.load_object(postings_file)
	return posting


def strip_and_preprocess(line):
	line = line.strip('" ')
	line = utility.tokenize(line)
	line = utility.remove_punctuations(line)
	line = utility.remove_stopwords(line)
	line = utility.stem(line)
	return line


def vsm(query, dictionary, posting_offset, start_offset, lengths):
	scores = {}
	query_weights = []
	for term, query_tf in query.items():
		if term in dictionary:
			dict_entry = dictionary.get(term)
			postings_entry = get_posting(dict_entry['index'], start_offset, posting_offset)
			idf = math.log10(len(lengths) / len(postings_entry))
			query_tf_weight = 1 + math.log10(query_tf)
			for doc_id, doc_tf in postings_entry:
				doc_tf_weight = 1 + math.log10(doc_tf)
				if doc_id not in scores:
					scores[doc_id] = 0
				scores[doc_id] += doc_tf_weight * idf * query_tf_weight
			query_weights.append(idf * query_tf_weight)

	query_l2_norm = math.sqrt(sum([math.pow(query_weight, 2) for query_weight in query_weights]))

	for doc_id, score in scores.items():
		scores[doc_id] /= lengths[str(doc_id)] * query_l2_norm

	#heapq by default is min heap, so * -1 to all score value
	scores_heap = [ScoreDocIDPair(-score, doc_id) for doc_id, score in scores.items()]
	heapq.heapify(scores_heap)

	return [heapq.heappop(scores_heap) for i in range(len(scores_heap))]


def process_query_into_ngram(phrase, n):
	ngrams = utility.generate_ngrams(phrase, n)
	return utility.count_tokens(ngrams)


def query_with_doc(doc_id):
	file_path = os.path.join(dir_doc, str(doc_id) + '.xml')
	if doc_id in doc_query_cache:
		pass
	elif os.path.isfile(file_path):
		doc_content = utility.extract_doc(file_path).get('content')
		doc_query_cache[doc_id] = handle_phrasal_query(doc_content)
	return doc_query_cache[doc_id]


def get_all_doc_ids(result):
	doc_ids = list(map(lambda x: x.doc_id, result))
	return doc_ids


def handle_phrasal_query(phrase):
	phrase = strip_and_preprocess(phrase)
	if len(phrase) >= 2:
		print('bigram case')
		processed_query = process_query_into_ngram(phrase, 2)
		result = vsm(processed_query, bigram_dict, bigram_postings_offset, bigram_start_offset, bigram_lengths)
	else:
		print('unigram case')
		processed_query = process_query_into_ngram(phrase, 1)
		result = vsm(processed_query, unigram_dict, unigram_postings_offset, unigram_start_offset, unigram_lengths)
	return result


def handle_boolean_query(query):
	phrases = query.split('AND')
	query_expansion_results = []
	for phrase in phrases:
		query_expansion_result = []
		result = handle_phrasal_query(phrase)
		for index, doc_id in enumerate(get_all_doc_ids(result)):
			print('\nquery expansion with doc', doc_id, '(', index + 1, ' / ', len(result), ')')
			query_expansion_result.append(query_with_doc(doc_id))
			print('query expansion result size: ', len(query_expansion_result[-1]))
		query_expansion_results.append(query_expansion_result)

	# TODO do reciprocal with results and query_expansion_results

	f = POST_PROCESSOR_DIR
	with open(f, 'wb') as f:
		utility.save_object(query_expansion_results, f)


def main():
	global unigram_dict, bigram_dict
	global unigram_lengths, bigram_lengths
	global unigram_postings_offset, bigram_postings_offset
	global postings_file, unigram_start_offset, bigram_start_offset

	with open(dict_path, 'rb') as f:
		unigram_dict = utility.load_object(f)
		bigram_dict = utility.load_object(f)
	print('dict loaded')

	with open(lengths_path, 'rb') as f:
		unigram_lengths = utility.load_object(f)
		bigram_lengths = utility.load_object(f)
	print('lengths loaded')

	postings_file = open(postings_path, 'rb')

	unigram_postings_offset = utility.load_object(postings_file)
	unigram_postings_offset.insert(0, 0)
	unigram_start_offset = postings_file.tell()
	postings_file.seek(unigram_postings_offset[-1], 1)

	bigram_postings_offset = utility.load_object(postings_file)
	bigram_postings_offset.insert(0, 0)
	bigram_start_offset = postings_file.tell()
	postings_file.seek(bigram_postings_offset[-1], 1)

	print('posting loaded')

	with open(query_path, 'r') as f:
		for line in f:
			line = line.strip()
			print('###QUERY###', line)
			if line != '':
				result = handle_boolean_query(line)

	postings_file.close()
	print('completed')


def usage():
	print("usage: " + sys.argv[0] + "-i directory-of-documents -d dictionary-file -p postings-file -q file-of-queries -l lengths-file -o output-file-of-results")


if __name__ == '__main__':
	dict_path = postings_path = query_path = output_path = lengths_path = None
	try:
		opts, args = getopt.getopt(sys.argv[1:], 'i:d:p:q:o:l:')
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
		elif o == '-q':
			query_path = a
		elif o == '-o':
			output_path = a
		elif o == '-l':
			lengths_path = a
		else:
			assert False, "unhandled option"
	if dir_doc is None or dict_path is None or postings_path is None or query_path is None or output_path is None or lengths_path is None:
		usage()
		sys.exit(2)

	main()
