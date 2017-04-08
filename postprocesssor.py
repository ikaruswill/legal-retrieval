import heapq

import utility
from utility import ScoreDocIDPair


POST_PROCESSOR_DIR = './query_exp_results.txt'

AVERAGING_POLICY = 0
SUMMATION_POLICY = 1
MEAN_RECIPROCAL_RANK_POLICY = 2
RANKING_POLICY = SUMMATION_POLICY

SAMPLE_SIZE = 20


# averaging
def apply_ranking_policy(processed_record, number_of_queries):
	if RANKING_POLICY == SUMMATION_POLICY:
		pass
	elif RANKING_POLICY == AVERAGING_POLICY:
		processed_record['score_id_pair'].score /= processed_record['count']
	elif RANKING_POLICY == MEAN_RECIPROCAL_RANK_POLICY:
		processed_record['score_id_pair'].score = processed_record['mrr'] * -1  # bc of heapq
		processed_record['score_id_pair'].score /= number_of_queries  # will not affect the ranking but by definition
	else:
		print("Unknown policy selected. SUMMATION POLICY applied.")
	return processed_record['score_id_pair']


def postprocess(query_expansion_results):
	number_of_queries = len(query_expansion_results)
	print('# of documents queried on query expansion: ', number_of_queries)

	processed_records = {}
	for query_expansion_result in query_expansion_results:
		for document_query_result in query_expansion_result:
			for rank, score_doc_id_pair in enumerate(document_query_result):
				if score_doc_id_pair.doc_id not in processed_records:
					processed_records[score_doc_id_pair.doc_id] = {'score_id_pair': score_doc_id_pair, 'count': 0,
																   'mrr': 0}
				processed_record = processed_records[score_doc_id_pair.doc_id]
				processed_record['score_id_pair'].score += score_doc_id_pair.score
				processed_record['count'] += 1
				processed_record['mrr'] += (1.0 / (rank + 1))

	for doc_id in processed_records:
		processed_records[doc_id] = apply_ranking_policy(processed_records[doc_id], number_of_queries)

	scores_heap = list(processed_records.values())
	heapq.heapify(scores_heap)
	result = [heapq.heappop(scores_heap) for _ in range(len(scores_heap))]
	return result


def main():
	f = open(POST_PROCESSOR_DIR, 'rb')
	query_expansion_results = utility.load_object(f)
	f.close()

	result = postprocess(query_expansion_results)
	print('Result size: ', len(result))
	print(list(map(lambda x: x.doc_id, result[:SAMPLE_SIZE])))

if __name__ == '__main__':
	main()
