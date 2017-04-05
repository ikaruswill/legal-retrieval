import utility
from utility import ScoreDocIDPair


POST_PROCESSOR_DIR = './query_exp_results'


def main():
	f = open(POST_PROCESSOR_DIR, 'rb')
	query_expansion_results = utility.load_object(f)
	print(len(query_expansion_results))
	for query_expansion_result in query_expansion_results:
		for phrasal_query_result in query_expansion_result:
			print(phrasal_query_result)
		break

	f.close()

if __name__ == '__main__':
	main()
