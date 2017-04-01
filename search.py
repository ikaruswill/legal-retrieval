import getopt
import sys
import pickle
import utility

def get_posting(index, start_offset, postings_offsets):
	byte_offset = start_offset + postings_offsets[index]
	postings_file.seek(byte_offset, 0)
	posting = pickle.load(postings_file)
	return posting

def main():
	dictionary = utility.load_object(dict_path)
	lengths = utility.load_object(lengths_path)

	postings_file = open(postings_path, 'rb')
	postings_offsets = pickle.load(postings_file).insert(0, 0)
	start_offset = postings_file.tell()

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