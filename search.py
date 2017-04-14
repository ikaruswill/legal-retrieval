import getopt
import sys
import utility
import math
import heapq
import os
from utility import ScoreDocIDPair
from utility import ScoreTermPair
from functools import reduce

unigram_dict = {}
bigram_dict = {}
unigram_lengths = {}
bigram_lengths = {}

doc_query_cache = {}


# Extra file that we generated from index.py, this file contains the euclidean norm of the documents
LENGTHS_PATH = 'lengths.txt'

# Maximum number of documents used to run the query expansion
QUERY_EXPANSION_DOCUMENT_LIMIT = 10

# Maximum number of keywords extracted from a set of documents
QUERY_EXPANSION_KEYWORD_LIMIT = 10

# Number of times the bigram terms from the initial query is appended the list of extracted keywords
QUERY_ENHANCE = 10


# Given a File object and load unigram dictionary and bigram dictionary
def load_dicts(dict_file):
	dicts = []
	current_dict = {}
	offset = 0
	for term, diff in utility.objects_in(dict_file):
		if term is None and diff is None:
			dicts.append(current_dict)
			current_dict = {}
		else:
			offset += diff
			current_dict[term] = {'offset': offset}
	return tuple(dicts)


# Given term and unigram/bigram dictionary, return postings of the term if exists
def get_posting(term, dictionary):
	postings_file.seek(dictionary[term]['offset'])
	posting = utility.load_object(postings_file)
	return posting


# Remove space and double quote, then run preprocess(line), finally return result
def strip_and_preprocess(line):
	line = line.strip('" ')
	line = preprocess(line)
	return line


# Tokenize a string, remove punctuations and stopwords, then stem each token. Return a list of stemmed words
def preprocess(line):
	line = utility.tokenize(line)
	line = utility.remove_punctuations(line)
	line = utility.remove_stopwords(line)
	line = utility.stem(line)
	return line


# Given n-grams with count, unigram/bigram dictionary, unigram/bigram lengths (euclidean norm of documents)
# and top_k which indicate the number of desired documents in the final result.
# This method evaluate using vector space model LNC.LTC and return a list of ScoreDocIDPair
def vsm(query_ngrams, dictionary, lengths, top_k=sys.maxsize):
	scores = {}
	query_weights = []
	for term, query_tf in query_ngrams.items():
		if term in dictionary:
			# print('term in dict')
			postings_entry = get_posting(term, dictionary)
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

	# heapq by default is min heap, so * -1 to all score value
	scores_heap = [ScoreDocIDPair(-score, doc_id) for doc_id, score in scores.items()]
	heapq.heapify(scores_heap)

	return [heapq.heappop(scores_heap) for i in range(min(len(scores_heap), top_k))]


# Given a list of stemmed words and a number n to indicate the target gram,
# for i.e. if n is 1, unigram is generated, if n is 2, bigram is generated.
# Returning result include the counts of each n-gram term
def turn_query_into_ngram(phrase, n):
	ngrams = utility.generate_ngrams(phrase, n)
	return utility.count_tokens(ngrams)


# #DEPRECATED Initially we do query expansion using the whole document content as a query.
# This method return a list of ranked document ids
def query_with_doc(doc_id):
	file_path = os.path.join(dir_doc, str(doc_id) + '.xml')
	if doc_id in doc_query_cache:
		pass
	elif os.path.isfile(file_path):
		doc_content = utility.extract_doc(file_path).get('content')
		doc_query_cache[doc_id] = handle_phrasal_query(doc_content)
	return doc_query_cache[doc_id]


# Given a list of list of keywords, return the union of all the list of keywords
# i.e. a list of non duplicated keywords
def combine_keyword_sets(keyword_sets):
	return list(set(reduce(lambda x, y: x + y, keyword_sets)))


# Convert keywords into n-grams with count,
# in order to call vsm(query_ngrams, dictionary, lengths, top_k=sys.maxsize) method.
def query_with_bigram_keywords(keywords):
	ngrams = utility.count_tokens(keywords)
	return vsm(ngrams, bigram_dict, bigram_lengths)


# Given a list of document IDs, combine all the document content into one string,
# preprocess it into a list of stemmed words, then convert it into n-grams with counts, N.
# Walk through all the terms in the n-grams, assign a score with formula
# 	[TERM_FREQ_IN_N] * [INV_DOC_FREQ_OF_CORPUS] * [DOC_FREQ_OF_TERM_IN_N].
# Return top QUERY_EXPANSION_KEYWORD_LIMIT number of keywords.
def extract_keywords_from_docs(doc_ids):
	result = []
	combined_doc = ''
	for doc_id in doc_ids:
		file_path = os.path.join(dir_doc, str(doc_id) + '.xml')
		if os.path.isfile(file_path):
			doc_content = utility.extract_doc(file_path).get('content')
			combined_doc += doc_content + ' '

	# tokenize, remove stopwords and punctuations
	combined_doc = utility.remove_css_text(combined_doc)
	combined_doc = preprocess(combined_doc)
	query_ngrams = turn_query_into_ngram(combined_doc, 2)

	for term, query_tf in query_ngrams.items():
		# negative score as the heapq is a min heap, replace doc id to term in this case
		if term in bigram_dict:
			postings_entry = get_posting(term, bigram_dict)
			query_df = sum([1 if is_doc_id_in_postings(doc_id, postings_entry) else 0 for doc_id in doc_ids]) / QUERY_EXPANSION_DOCUMENT_LIMIT
			idf = math.log10(len(bigram_lengths) / len(postings_entry))
			tfidf = (1 + math.log10(query_tf)) * idf * query_df
			result.append(ScoreTermPair(-tfidf, term))

	heapq.heapify(result)

	return [heapq.heappop(result).term for i in range(min(QUERY_EXPANSION_KEYWORD_LIMIT, len(result)))]


# Given a document ID and a postings list, check if the document ID appears in the postings list
def is_doc_id_in_postings(target_doc_id, postings):
	for doc_id, _ in postings:
		if doc_id == target_doc_id:
			return True
		elif +doc_id > +target_doc_id:
			return False
	return False


# Given a list of ScoreDocIDPair, return a list of document ID (in other words, remove the score)
def get_all_doc_ids(result):
	return list(map(lambda x: x.doc_id, result))


# Given a list of stemmed words, turn it into bigrams with count and evaluate with vector space model
def handle_bigram_query(phrase):
	ngrams = turn_query_into_ngram(phrase, 2)
	return vsm(ngrams, bigram_dict, bigram_lengths, QUERY_EXPANSION_DOCUMENT_LIMIT)


# Given a list of stemmed words, turn it into unigram with count and evaluate with vector space model
def handle_unigram_query(phrase):
	ngrams = turn_query_into_ngram(phrase, 1)
	return vsm(ngrams, unigram_dict, unigram_lengths, QUERY_EXPANSION_DOCUMENT_LIMIT)


# Given a phrase, strip and preprocess it into a list of stemmed words,
# decide whether handle it using handle_unigram_query(phrase) or handle_bigram_query(phrase)
def handle_phrasal_query(phrase):
	processed_phrase = strip_and_preprocess(phrase)
	if len(processed_phrase) >= 2:
		return handle_bigram_query(processed_phrase)
	else:
		return handle_unigram_query(processed_phrase)


# Given a list of phrases, turn all of them into a bigram if their word count is bigger than 1 and return the result
def convert_phrases_into_bigrams(phrases):
	results = []
	for phrase in phrases:
		processed_phrase = strip_and_preprocess(phrase)
		if len(processed_phrase) >= 2:
			results += list(dict(turn_query_into_ngram(processed_phrase, 2)).keys())
	return results


# Check whether the document has all the keywords. Return 0 if doesn't. 1 if has.
# Return Integer for ease of sorting.
def have_all_keywords(doc_id, keywords):
	file_path = os.path.join(utility.load_config().get('dir_doc'), str(doc_id) + '.xml')
	entities = utility.extract_doc(file_path)
	doc_content = entities.get('content')
	for keyword in keywords:
		if keyword not in doc_content:
			return 0
	return 1


# Sort boolean query ranking to prioritize documents with all the query keywords
def sort_by_boolean_query(ranking, keywords):
	keywords = list(map(lambda x: x.strip('" '), keywords))
	result = []
	for pair in ranking:
		doc_id = pair.doc_id
		result.append([pair, have_all_keywords(doc_id, keywords)])
	result.sort(key=lambda x: -x[1])
	result = list(map(lambda x: x[0], result))
	return result


# Given original query string with ‘AND’, split it into multiple phrases.
# Each phrase is evaluated with handle_phrasal_query(phrase).
# Keywords are extracted using intermediate result, then used to make query expansion.
# Final result is sorted against the occurrences of all original phrases.
def handle_boolean_query(query):
	phrases = query.split('AND')

	extracted_keyword_sets = []
	for phrase in phrases:
		result = handle_phrasal_query(phrase)
		all_doc_ids = get_all_doc_ids(result)
		extracted_keyword_sets.append(extract_keywords_from_docs(all_doc_ids))

	combined_keywords = combine_keyword_sets(extracted_keyword_sets)
	query_bigram_terms = convert_phrases_into_bigrams(phrases)
	for i in range(0, QUERY_ENHANCE):
		combined_keywords += query_bigram_terms
	final_ranking = query_with_bigram_keywords(combined_keywords)

	final_ranking = sort_by_boolean_query(final_ranking, phrases)
	return map(lambda x: x.doc_id, final_ranking)


def main():
	global unigram_dict, bigram_dict
	global unigram_lengths, bigram_lengths
	global postings_file

	postings_file = open(postings_path, 'rb')

	with open(dict_path, 'rb') as f:
		unigram_dict, bigram_dict = load_dicts(f)

	with open(LENGTHS_PATH, 'rb') as f:
		unigram_lengths = utility.load_object(f)
		bigram_lengths = utility.load_object(f)

	result = []
	with open(query_path, 'r') as f:
		for line in f:
			line = line.strip()
			if line != '':
				result = handle_boolean_query(line)

	output = ' '.join(list(map(lambda x: str(x), result)))
	with open(output_path, 'w') as f:
		f.write(output)

	postings_file.close()


def usage():
	print("usage: " + sys.argv[0] + "-d dictionary-file -p postings-file -q file-of-queries -o output-file-of-results")

if __name__ == '__main__':
	dict_path = postings_path = query_path = output_path = None
	try:
		opts, args = getopt.getopt(sys.argv[1:], 'd:p:q:o:')
	except getopt.GetoptError as err:
		usage()
		sys.exit(2)
	for o, a in opts:
		if o == '-d':
			dict_path = a
		elif o == '-p':
			postings_path = a
		elif o == '-q':
			query_path = a
		elif o == '-o':
			output_path = a
		else:
			assert False, "unhandled option"

	args = utility.load_config()
	dir_doc = args.get('dir_doc')
	dict_path = args.get('dict_path', dict_path)
	postings_path = args.get('postings_path', postings_path)

	if dir_doc is None or dict_path is None or postings_path is None or query_path is None or output_path is None:
		usage()
		sys.exit(2)

	dir_doc += '/' if not dir_doc.endswith('/') else ''

	main()
