import getopt
import sys
import pickle
import utility
import math
import heapq

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

class ScoreDocIDPair(object):
	def __init__(self, score, doc_id):
		self.score = score
		self.doc_id = doc_id

	def __lt__(self, other):
		return int(self.doc_id) < int(other.doc_id) if self.score == other.score else self.score < other.score

	def __repr__(self):
		return '%6s : %.10f' % (self.doc_id, self.score)

	def __str__(self):
		return '%6s : %.10f' % (self.doc_id, self.score)

def get_posting(index, start_offset, postings_offsets):
	byte_offset = start_offset + postings_offsets[index]
	postings_file.seek(byte_offset, 0)
	posting = pickle.load(postings_file)
	return posting

def strip_and_preproces(line):
	key = 'content'
	#treat query as doc in order to preprocess
	doc = {}
	doc[key] = line.strip('" ')
	utility.tokenize([doc], key)
	utility.remove_punctuations([doc], key)
	utility.remove_stopwords([doc], key)
	utility.stem([doc], key)
	return doc[key]

def vsm(term, dictionary, posting_offset, start_offset, lengths):
	scores = {}
	query_weights = []
	if term in dictionary:
		# print('term in dict')
		dict_entry = dictionary.get(term)
		# print('dict entry', dict_entry)
		postings_entry = get_posting(dict_entry['index'], start_offset, posting_offset)
		# print('posting entry', postings_entry)
		idf = math.log10(len(lengths) / len(postings_entry))
		for doc_id, doc_tf in postings_entry:
			doc_tf_weight = 1 + math.log10(doc_tf)
			if doc_id not in scores:
				scores[doc_id] = 0
			scores[doc_id] += doc_tf_weight * idf # query 1 + log(query_tf) always 1
		query_weights.append(idf)

	query_l2_norm = math.sqrt(sum([math.pow(query_weight, 2) for query_weight in query_weights]))

	# print('lengths', lengths)
	# print('scores', scores)
	for doc_id, score in scores.items():
		scores[doc_id] /= lengths[str(doc_id)] * query_l2_norm

	#heapq by default is min heap, so * -1 to all score value
	scores_heap = [ScoreDocIDPair(-score, doc_id) for doc_id, score in scores.items()]
	heapq.heapify(scores_heap)

	return [heapq.heappop(scores_heap) for i in range(len(scores_heap))]

def handle_query(query):
	phrases = list(map(strip_and_preproces, query.split('AND')))
	result = []
	for phrase in phrases:
		term = ' '.join(phrase)
		print('term:', term)
		if len(phrase) == 3:
			print('trigram case')
			result.append(vsm(term, trigram_dict, trigram_postings_offset, trigram_start_offset, trigram_lengths))
		elif len(phrase) == 2:
			print('bigram case')
			result.append(vsm(term, bigram_dict, bigram_postings_offset, bigram_start_offset, bigram_lengths))
		else:
			print('unigram case')
			result.append(vsm(term, unigram_dict, unigram_postings_offset, unigram_start_offset, unigram_lengths))
	print('result', result)

def main():
	global unigram_dict, bigram_dict, trigram_dict
	global unigram_lengths, bigram_lengths, trigram_lengths
	global unigram_postings_offset, bigram_postings_offset, trigram_postings_offset
	global postings_file, unigram_start_offset, bigram_start_offset, trigram_start_offset

	with open(dict_path, 'rb') as f:
		bunigram_dict = pickle.load(f)
		bigram_dict = pickle.load(f)
		trigram_dict = pickle.load(f)
		f.close()
	print('dict loaded')

	with open(lengths_path, 'rb') as f:
		unigram_lengths = pickle.load(f)
		bigram_lengths = pickle.load(f)
		trigram_lengths = pickle.load(f)
		f.close()
	print('lengths loaded')

	postings_file = open(postings_path, 'rb')

	unigram_postings_offset = pickle.load(postings_file)
	unigram_postings_offset.insert(0, 0)
	unigram_start_offset = postings_file.tell()
	postings_file.seek(unigram_postings_offset[-1], 1)

	bigram_postings_offset = pickle.load(postings_file)
	bigram_postings_offset.insert(0, 0)
	bigram_start_offset = postings_file.tell()
	postings_file.seek(bigram_postings_offset[-1], 1)

	trigram_postings_offset = pickle.load(postings_file)
	trigram_postings_offset.insert(0, 0)
	trigram_start_offset = postings_file.tell()
	print('posting loaded')

	with open(query_path, 'r') as f:
		for line in f:
			line = line.strip()
			print('###QUERY###', line)
			if line != '':
				result = handle_query(line)
		f.close()

	postings_file.close()

def usage():
	print("usage: " + sys.argv[0] + " -d dictionary-file -p postings-file -q file-of-queries -l lengths-file -o output-file-of-results")

if __name__ == '__main__':
	dict_path = postings_path = query_path = output_path = lengths_path = None
	try:
		opts, args = getopt.getopt(sys.argv[1:], 'd:p:q:o:l:')
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
		elif o == '-l':
			lengths_path = a
		else:
			assert False, "unhandled option"
	if dict_path == None or postings_path == None or query_path == None or output_path == None or lengths_path == None:
		usage()
		sys.exit(2)

	main()
