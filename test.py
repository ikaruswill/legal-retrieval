import utility
import os
import sys
import re
import diff

# if len(sys.argv) < 2:
#     print('missing doc id')
#     sys.exit(2)
#
# file_path = os.path.join('./doc_with_css', str(sys.argv[1]) + '.xml')
# if os.path.isfile(file_path):
#     doc_content = utility.extract_doc(file_path).get('content')
#     output_file = open('./diff/' + str(sys.argv[1]) + 'diff.xml', 'w')
#     d1 = doc_content.split('\n')
#     d2 = utility.remove_css_text(doc_content).split('\n')
#     diff.write_diff_to_file(d1, d2, output_file)
#     output_file.close()


for dirpath, dirnames, filenames in os.walk('./doc_with_css'):
    i = 1
    for name in sorted(filenames):
        if name.endswith('.xml'):
            print('processing #', i, name)
            i += 1
            file_path = os.path.join(dirpath, name)
            entities = utility.extract_doc(file_path)
            doc_content = entities.get('content')
            output_file = open('./diff/' + name, 'w')
            d1 = doc_content.split('\n')
            d2 = utility.remove_css_text(doc_content).split('\n')
            diff.write_diff_to_file(d1, d2, output_file)
            output_file.close()

# check if there's line > certain length, else remove file
# for dirpath, dirnames, filenames in os.walk('./diff'):
#     for name in sorted(filenames):
#         # print('processing', name)
#         if name.endswith('.xml'):
#             file_path = os.path.join(dirpath, name)
#             doc_file = open(file_path, 'r')
#
#             keep_doc = False
#             hasLine = False
#             for line in doc_file:
#                 hasLine = True
#                 if len(line) > 40:
#                     print('******CHECK DOC*******', name)
#                     keep_doc = True
#                     break
#
#             if not hasLine:
#                 print('******CHECK DOC*******', name)
#
#             if not keep_doc:
#                 os.remove(file_path)
