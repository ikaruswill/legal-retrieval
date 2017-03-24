import getopt
import sys
import os
import xml.etree.ElementTree

def str2bool(bool_str):
	return bool_str.lower() in ("yes", "true", "t", "1")

def parse_child(child):
	if child.tag == 'str':
		return child.text
	elif child.tag == 'date':
		return child.text # Can do date parsing
	elif child.tag == 'bool':
		return str2bool(child.text)
	elif child.tag == 'long':
		return long(child.text)
	elif child.tag == 'float':
		return float(child.text)
	elif child.tag == 'arr':
		arr = []
		for grandchild in child:
			arr.append(parse_child)
		return arr
	else:
		exit('Unsupported tag: ', child.tag)

def extract_doc(file_path):
	doc = {}
	root = xml.etree.ElementTree.parse(file_path).getroot()
	for child in root:
		key = child.attrib['name']
		doc[key] = parse_child(child)

	return doc
def load_data(dir_doc):
	docs = {}
	for dirpath, dirnames, filenames in os.walk(dir_doc):
		for name in filenames:
			if name.endswith('.xml'):
				file_path = os.path.join(dirpath, name)
				doc_id = os.path.splitext(name)[0]
				docs[name] = extract_doc(file_path)

	return docs

def usage():
	print("usage: " + sys.argv[0] + " -i directory-of-documents -d dictionary-file -p postings-file -l lengths-file")

def main():
	docs = load_data(dir_doc)

if __name__ == '__main__':
	dir_doc = dict_path = postings_path = lengths_path = None
	try:
		opts, args = getopt.getopt(sys.argv[1:], 'i:d:p:l:')
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
		elif o == '-l':
			lengths_path = a
		else:
			assert False, "unhandled option"
	if dir_doc == None or dict_path == None or postings_path == None or lengths_path == None:
		usage()
		sys.exit(2)

	main()
