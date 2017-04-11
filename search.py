import getopt
import sys
import utility
import math
import heapq
import os
import operator
from utility import ScoreDocIDPair

import postprocesssor

doc_query_cache = {}

POST_PROCESSOR_DIR = './query_exp_results.txt'
LENGTHS_PATH = 'lengths.txt'


def load_dict(dict_file):
	dictionary = {}
	offset = 0
	for term, diff in utility.objects_in(dict_file):
		offset += diff
		dictionary[term] = {'offset': offset}
	return dictionary


def get_postings(term_tokens):
	if len(term_tokens) == 1:
		return load_postings(term)
	elif len(term_tokens) == 2:
		return get_biword_postings(term_tokens, dictionary)
	else:
		pass


def load_postings(token):
	postings_file.seek(dictionary[token]['offset'])
	postings = utility.load_object(postings_file)
	return postings


def get_biword_postings(tokens, dictionary):
	l_token, r_token = tokens[0], tokens[1]
	l_postings = load_postings(l_token)
	r_postings = load_postings(r_token)
	posting_pairs = walk_and_retrieve(l_postings, r_postings, key=operator.itemgetter(0))
	biword_postings = []

	for l_posting, r_posting in posting_pairs:
		doc_id, l_positions = l_posting
		r_positions = r_posting[1]
		position_pairs = walk_and_retrieve(l_positions, r_positions, diff=1)
		biword_postings.append((doc_id, position_pairs,))
	return biword_postings


def walk_and_retrieve(l_list, r_list, key=lambda x:x, item=lambda x:x, diff=0):
	l_list = iter(l_list)
	r_list = iter(r_list)
	res = []
	l = next(l_list)
	r = next(r_list)

	while True:
		try:
			key_diff = key(r) - key(l)
			if key_diff == diff:
				res.append((item(l), item(r),))
				l = next(l_list)
				r = next(r_list)
			elif key_diff > diff:
				next(l)
			else:
				next(r)
		except StopIteration:
			return res


def strip_and_preprocess(line):
	line = line.strip('" ')
	line = utility.tokenize(line)
	line = utility.remove_punctuations(line)
	line = utility.remove_stopwords(line)
	line = utility.stem(line)
	return line


def vsm(phrase_tokens):
	scores = {}
	query_weights = []
	for term, query_tf in query.items():
		if term in dictionary:
			# print('term in dict')
			postings_entry = get_postings(term, dictionary)
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

	for doc_id, score in scores.items():
		scores[doc_id] /= lengths[doc_id] * query_l2_norm

	#heapq by default is min heap, so * -1 to all score value
	scores_heap = [ScoreDocIDPair(-score, doc_id) for doc_id, score in scores.items()]
	heapq.heapify(scores_heap)

	return [heapq.heappop(scores_heap) for i in range(len(scores_heap))]

def query_with_doc(doc_id):
	file_path = os.path.join(dir_doc, str(doc_id) + '.xml')
	if doc_id in doc_query_cache:
		pass
	elif os.path.isfile(file_path):
		doc_content = utility.extract_doc(file_path).get('content')
		doc_query_cache[doc_id] = handle_phrasal_query(doc_content)
	return doc_query_cache[doc_id]


def get_all_doc_ids(result):
	return list(map(lambda x: x.doc_id, result))


def handle_phrasal_query(phrase):
	phrase_tokens = strip_and_preprocess(phrase)
	result = vsm(phrase_tokens)
	return result


def handle_boolean_query(query):
	phrases = query.split('AND')
	boolean_query_results = []
	for phrase in phrases:
		query_expansion_result = []
		result = handle_phrasal_query(phrase)
		for index, doc_id in enumerate(get_all_doc_ids(result)):
			print('\nquery expansion with doc', doc_id, '(', index + 1, ' / ', len(result), ')')
			query_expansion_result.append(query_with_doc(doc_id))
			print('query expansion result size: ', len(query_expansion_result[-1]))
		boolean_query_results.append(query_expansion_result)

	# # development purpose since postprocessor.py is much faster than search.py
	# f = POST_PROCESSOR_DIR
	# with open(f, 'wb') as f:
	# 	utility.save_object(boolean_query_results, f)

	results = postprocesssor.postprocess(boolean_query_results)
	return results


def main():
	global dictionary, lengths
	global postings_file

	postings_file = open(postings_path, 'rb')
	print('posting opened')

	with open(dict_path, 'rb') as f:
		dictionary = load_dict(f)
	print('dict loaded')

	with open(LENGTHS_PATH, 'rb') as f:
		lengths = utility.load_object(f)
	print('lengths loaded')

	result = []
	with open(query_path, 'r') as f:
		for line in f:
			line = line.strip()
			print('###QUERY###', line)
			if line != '':
				result = handle_boolean_query(line)

	output = ' '.join(list(map(lambda x: str(x.doc_id), result)))
	with open(output_path, 'w') as f:
		f.write(output)

	postings_file.close()
	print('completed')


def usage():
	print("usage: " + sys.argv[0] + "-i directory-of-documents -d dictionary-file -p postings-file -q file-of-queries -l lengths-file -o output-file-of-results")

if __name__ == '__main__':
	dir_doc = dict_path = postings_path = query_path = output_path = None
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
		else:
			assert False, "unhandled option"
	if dir_doc is None or dict_path is None or postings_path is None or query_path is None or output_path is None:
		usage()
		sys.exit(2)

	dir_doc += '/' if not dir_doc.endswith('/') else ''

	main()
