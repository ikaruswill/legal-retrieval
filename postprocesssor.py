import heapq

import utility
from utility import ScoreDocIDPair


POST_PROCESSOR_DIR = './query_exp_results.txt'

AVERAGING_POLICY = 0
SUMMATION_POLICY = 1
MEAN_RECIPROCAL_RANK_POLICY = 2
QUERY_EXPANSION_RANKING_POLICY = MEAN_RECIPROCAL_RANK_POLICY
BOOLEAN_QUERY_RANKING_POLICY = MEAN_RECIPROCAL_RANK_POLICY

SAMPLE_SIZE = 20


# averaging
def apply_ranking_policy(processed_record, number_of_queries, policy):
	if policy == SUMMATION_POLICY:
		pass
	elif policy == AVERAGING_POLICY:
		processed_record['score_id_pair'].score /= processed_record['count']
	elif policy == MEAN_RECIPROCAL_RANK_POLICY:
		processed_record['score_id_pair'].score = processed_record['mrr'] * -1  # bc of heapq
		processed_record['score_id_pair'].score /= number_of_queries  # will not affect the ranking but by definition
	else:
		print("Unknown policy selected. SUMMATION POLICY applied.")
	return processed_record['score_id_pair']


def combine_rankings(rankings, policy):
	number_of_queries = len(rankings)
	print('# of rankings combined: ', number_of_queries)
	processed_records = {}
	for ranking in rankings:
		for rank, score_doc_id_pair in enumerate(ranking):
			if score_doc_id_pair.doc_id not in processed_records:
				processed_records[score_doc_id_pair.doc_id] = {'score_id_pair': score_doc_id_pair, 'count': 0,
															   'mrr': 0}
			processed_record = processed_records[score_doc_id_pair.doc_id]
			processed_record['score_id_pair'].score += score_doc_id_pair.score
			processed_record['count'] += 1
			processed_record['mrr'] += (1.0 / (rank + 1))
			processed_records[score_doc_id_pair.doc_id] = processed_record
	for doc_id in processed_records:
		processed_records[doc_id] = apply_ranking_policy(processed_records[doc_id], number_of_queries, policy)

	scores_heap = list(processed_records.values())
	heapq.heapify(scores_heap)
	result = [heapq.heappop(scores_heap) for _ in range(len(scores_heap))]

	return result


def postprocess(boolean_query_results):
	phrasal_query_rankings = []
	for query_expansion_result in boolean_query_results:
		phrasal_query_rankings.append(combine_rankings(query_expansion_result, QUERY_EXPANSION_RANKING_POLICY))
	combined_ranking = combine_rankings(phrasal_query_rankings, BOOLEAN_QUERY_RANKING_POLICY)
	return combined_ranking


def main():
	f = open(POST_PROCESSOR_DIR, 'rb')
	boolean_query_results = utility.load_object(f)
	f.close()

	result = postprocess(boolean_query_results)
	print('Result size: ', len(result))
	print(list(map(lambda x: x.doc_id, result[:SAMPLE_SIZE])))

if __name__ == '__main__':
	main()
