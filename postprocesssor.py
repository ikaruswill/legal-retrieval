import heapq

import utility
from utility import ScoreDocIDPair


POST_PROCESSOR_DIR = './query_exp_results'

AVERAGING_POLICY = 0
SUMMATION_POLICY = 1
RECIPROCAL_RANK_POLICY = AVERAGING_POLICY

SAMPLE_SIZE = 20


# averaging
def apply_reciprocal_rank_policy(processed_record):
	if RECIPROCAL_RANK_POLICY == SUMMATION_POLICY:
		pass
	elif RECIPROCAL_RANK_POLICY == AVERAGING_POLICY:
		processed_record['score_id_pair'].score /= processed_record['count']
	return processed_record['score_id_pair']


def postprocess(query_expansion_results):
	print('# of documents queried on query expansion: ', len(query_expansion_results))

	processed_records = {}
	for query_expansion_result in query_expansion_results:
		for phrasal_query_result in query_expansion_result:
			for score_doc_id_pair in phrasal_query_result:
				if score_doc_id_pair.doc_id not in processed_records:
					processed_records[score_doc_id_pair.doc_id] = {'score_id_pair': score_doc_id_pair, 'count': 0}
				processed_record = processed_records[score_doc_id_pair.doc_id]
				processed_record['count'] += 1
				processed_record['score_id_pair'].score += score_doc_id_pair.score

	for doc_id in processed_records:
		processed_records[doc_id] = apply_reciprocal_rank_policy(processed_records[doc_id])

	scores_heap = list(processed_records.values())
	heapq.heapify(scores_heap)
	result = [heapq.heappop(scores_heap) for i in range(len(scores_heap))]
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
