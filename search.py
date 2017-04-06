import getopt
import sys
import utility
from utility import ScoreDocIDPair
import math
import heapq
import os

unigram_dict = {}
bigram_dict = {}
trigram_dict = {}
unigram_lengths = {}
bigram_lengths = {}
trigram_lengths = {}
unigram_postings_offset = {}
bigram_postings_offset = {}
trigram_postings_offset = {}
postings_file = {}
unigram_start_offset = 0
bigram_start_offset = 0
trigram_start_offset = 0

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
			# print('term in dict')
			dict_entry = dictionary.get(term)
			# print('dict entry', dict_entry)
			postings_entry = get_posting(dict_entry['index'], start_offset, posting_offset)
			# print('posting entry', postings_entry)
			idf = math.log10(len(lengths) / len(postings_entry))
			query_tf_weight = 1 + math.log10(query_tf)
			for doc_id, doc_tf in postings_entry:
				doc_tf_weight = 1 + math.log10(doc_tf)
				if doc_id not in scores:
					scores[doc_id] = 0
				scores[doc_id] += doc_tf_weight * idf * query_tf_weight
			query_weights.append(idf * query_tf_weight)

	query_l2_norm = math.sqrt(sum([math.pow(query_weight, 2) for query_weight in query_weights]))

	# print('lengths', lengths)
	# print('scores', scores)
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
	if os.path.isfile(file_path):
		doc_content = utility.extract_doc(file_path).get('content')
		return handle_query(doc_content, False)

def get_all_doc_ids(results):
	# just simply OR for now
	doc_ids = set()
	for result in results:
		doc_ids = doc_ids.union(list(map(lambda x: x.doc_id, result)))
	return doc_ids

def handle_query(query, query_expansion=True):
	if query_expansion:
		query = query.split('AND')
	else:
		query = [query]
	phrases = list(map(strip_and_preprocess, query))
	results = []
	for phrase in phrases:
		if len(phrase) >= 3: #three words original query or documents with more than 3 words
			print('trigram case')
			processed_query = process_query_into_ngram(phrase, 3)
			results.append(vsm(processed_query, trigram_dict, trigram_postings_offset, trigram_start_offset, trigram_lengths))
		elif len(phrase) == 2:
			print('bigram case')
			processed_query = process_query_into_ngram(phrase, 2)
			results.append(vsm(processed_query, bigram_dict, bigram_postings_offset, bigram_start_offset, bigram_lengths))
		else:
			print('unigram case')
			processed_query = process_query_into_ngram(phrase, 1)
			results.append(vsm(processed_query, unigram_dict, unigram_postings_offset, unigram_start_offset, unigram_lengths))

	if not query_expansion:
		return results

	print("Init Query: ", results)
	print('result size: ', ', '.join(map(lambda x: str(len(x)), results)))

	query_expansion_results = []
	for doc_id in get_all_doc_ids(results):
		print('query expansion with doc', doc_id)
		query_expansion_results.append(query_with_doc(doc_id))
		print('query expansion result size: ', ', '.join(map(lambda x: str(len(x)), query_expansion_results[-1])))
	# TODO do reciprocal with results and query_expansion_results

	f = POST_PROCESSOR_DIR
	with open(f, 'wb') as f:
		utility.save_object(query_expansion_results, f)


def main():
	global unigram_dict, bigram_dict, trigram_dict
	global unigram_lengths, bigram_lengths, trigram_lengths
	global unigram_postings_offset, bigram_postings_offset, trigram_postings_offset
	global postings_file, unigram_start_offset, bigram_start_offset, trigram_start_offset

	with open(dict_path, 'rb') as f:
		unigram_dict = utility.load_object(f)
		bigram_dict = utility.load_object(f)
		trigram_dict = utility.load_object(f)
	print('dict loaded')

	with open(lengths_path, 'rb') as f:
		unigram_lengths = utility.load_object(f)
		bigram_lengths = utility.load_object(f)
		trigram_lengths = utility.load_object(f)
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

	trigram_postings_offset = utility.load_object(postings_file)
	trigram_postings_offset.insert(0, 0)
	trigram_start_offset = postings_file.tell()
	print('posting loaded')

	with open(query_path, 'r') as f:
		for line in f:
			line = line.strip()
			print('###QUERY###', line)
			if line != '':
				result = handle_query(line)

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
	if dir_doc == None or dict_path == None or postings_path == None or query_path == None or output_path == None or lengths_path == None:
		usage()
		sys.exit(2)

	main()
