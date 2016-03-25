__author__ = 'tracy'

import json
import ast

# fr=open("meta_Beauty.json")
# fw=open("fixed_meta_Beauty.json", "w")
#
# for line in fr:
#     json_dat = json.dumps(ast.literal_eval(line))
#     dict_dat = json.loads(json_dat)
#     json.dump(dict_dat, fw)
#     fw.write("\n")
#
# fw.close()
# fr.close()

fr=open("reviews_Beauty.json")
fw=open("fixed_reviews_Beauty.json", "w")

for line in fr:
    json_dat = json.dumps(ast.literal_eval(line))
    dict_dat = json.loads(json_dat)
    json.dump(dict_dat, fw)
    fw.write("\n")

fw.close()
fr.close()