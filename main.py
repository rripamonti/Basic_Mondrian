"""
run basic_mondrian with given parameters
"""

# !/usr/bin/env python
# coding=utf-8
from mondrian import mondrian
from utils.read_adult_data import read_data as read_adult
from utils.read_adult_data import read_tree as read_adult_tree
from utils.read_informs_data import read_data as read_informs
from utils.read_informs_data import read_tree as read_informs_tree
from utils.read_adult_data import get_att_names
from utils.read_adult_data import write_reorder_anonymized_data
import sys, copy, random

# sys.setrecursionlimit(50000)


def extend_result(val):
    """
    separated with ',' if it is a list
    """
    if isinstance(val, list):
        return ','.join(val)
    return val


def write_to_file(result,k):
    """
    write the anonymized result to anonymized.data
    """
    with open("data/anonymized_"+str(k)+".data", "w") as output:
        output.write(';'.join(get_att_names())+'\n')
        for r in result:
            output.write(';'.join(map(extend_result, r)) + '\n')
    write_reorder_anonymized_data("data/anonymized_"+str(k)+".data",k)


def get_result_one(att_trees, data, k):
    "run basic_mondrian for one time, with k=10"
    print "K=%d" % k
    print "Mondrian"
    result, eval_result = mondrian(att_trees, data, k)
    write_to_file(result,k)
    print "NCP %0.2f" % eval_result[0] + "%"
    print "Running time %0.2f" % eval_result[1] + "seconds"

if __name__ == '__main__':
    print '#' * 30
    print "Adult data"
    i = [5,10,15,20]
    for k in i:
        RAW_DATA = read_adult()
        ATT_TREES = read_adult_tree()
        get_result_one(ATT_TREES, RAW_DATA,k)

    # anonymized dataset is stored in result
    print "Finish Basic_Mondrian!!"
