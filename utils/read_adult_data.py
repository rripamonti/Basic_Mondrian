#!/usr/bin/env python
# coding=utf-8

# Read data and read tree fuctions for INFORMS data
# attributes ['age', 'workcalss', 'final_weight', 'education', 'education_num',
# 'marital_status', 'occupation', 'relationship', 'race', 'sex', 'capital_gain',
# 'capital_loss', 'hours_per_week', 'native_country', 'class']
# QID ['age', 'workcalss', 'education', 'matrital_status', 'race', 'sex', 'native_country']
# SA ['occopation']
from models.gentree import GenTree
from models.numrange import NumRange
from utils.utility import cmp_str
import pickle
import pandas as pd
import pdb

ATT_NAMES = ['age', 'workclass', 'final_weight', 'education',
             'education_num', 'marital_status', 'occupation', 'relationship',
             'race', 'sex', 'capital_gain', 'capital_loss', 'hours_per_week',
             'native_country', 'class']
# 8 attributes are chose as QI attributes
# age and education levels are treated as numeric attributes
# only matrial_status and workclass has well defined generalization hierarchies.
# other categorical attributes only have 2-level generalization hierarchies.
QI_INDEX = [0, 1, 3, 5, 6, 8, 9, 13, 14]
IS_CAT = [False, True, True, True, True, True, True, True, True]
SA_INDEX = -1

ATT_NAMES_REORDERED = ['age', 'workclass', 'final_weight', 'education',
             'education_num', 'marital_status', 'occupation', 'relationship',
             'race', 'sex', 'capital_gain', 'capital_loss', 'hours_per_week',
             'native_country', 'class']

__DEBUG = False


def read_data():
    """
    read microda for *.txt and return read data
    """
    QI_num = len(QI_INDEX)
    data = []
    numeric_dict = []
    for i in range(QI_num):
        numeric_dict.append(dict())
    # oder categorical attributes in intuitive order
    # here, we use the appear number
    data_file = open('data/adult.data', 'rU')
    for line in data_file:
        line = line.strip()
        # remove empty and incomplete lines
        # only 30162 records will be kept
        if len(line) == 0 or '?' in line:
            continue
        # remove double spaces
        line = line.replace(' ', '')
        temp = line.split(',')
        ltemp = []
        reordered_data = []
        #for i in range(QI_num): @RR2020
        #    index = QI_INDEX[i]
        #    if IS_CAT[i] is False:
        #        try:
        #            numeric_dict[i][temp[index]] += 1
        #        except KeyError:
        #            numeric_dict[i][temp[index]] = 1
        #    ltemp.append(temp[index])
        j=0
        for i in range(len(temp)):
            reordered_data.append("")
        for i in range(len(temp)):
            if i in QI_INDEX:
                if IS_CAT[QI_INDEX.index(i)] is False:
                    try:
                        numeric_dict[QI_INDEX.index(i)][temp[i]] += 1
                    except KeyError:
                        numeric_dict[QI_INDEX.index(i)][temp[i]] = 1
                reordered_data[QI_INDEX.index(i)] = temp[i]
                ATT_NAMES_REORDERED[QI_INDEX.index(i)] = ATT_NAMES[i]
            else:
                reordered_data[len(QI_INDEX)+j] = temp[i]
                ATT_NAMES_REORDERED[len(QI_INDEX)+j] = ATT_NAMES[i]
                j = j + 1
        for value in reordered_data:
            ltemp.append(value)
        #ltemp.append(temp[SA_INDEX])
        data.append(ltemp)

    return data

#@RR2020
def get_att_names():
    return ATT_NAMES_REORDERED

def read_tree():
    """read tree from data/tree_*.txt, store them in att_tree
    """
    att_names = []
    att_trees = []
    for t in QI_INDEX:
        att_names.append(ATT_NAMES[t])
    for i in range(len(att_names)):
        if IS_CAT[i]:
            att_trees.append(read_tree_file(att_names[i]))
        else:
            att_trees.append(read_numeric_identifier(att_names[i]))
    return att_trees


def read_numeric_identifier(att_name):
    """
    read pickle file for numeric attributes
    return numrange object
    """
    csvdata = pd.read_csv('data/adult.data',sep=',',header=0,names=ATT_NAMES)
    csvdata = csvdata.sort_values(by=att_name)
    csvdata = csvdata.dropna(subset=[att_name])
    sort_value = csvdata.age.astype('str').unique()
    result = NumRange(sort_value.tolist(), dict())
    return result


def read_tree_file(treename):
    """read tree data from treename
    """
    leaf_to_path = {}
    att_tree = {}
    prefix = 'data/adult_'
    postfix = ".csv"
    treefile = open(prefix + treename + postfix, 'rU')
    att_tree['*'] = GenTree('*')
    if __DEBUG:
        print "Reading Tree" + treename
    for line in treefile:
        # delete \n
        if len(line) <= 1:
            break
        line = line.strip()
        temp = line.split(';')
        # copy temp
        temp.reverse()
        for i, t in enumerate(temp):
            isleaf = False
            if i == len(temp) - 1:
                isleaf = True
            # try and except is more efficient than 'in'
            try:
                att_tree[t]
            except:
                att_tree[t] = GenTree(t, att_tree[temp[i - 1]], isleaf)
    if __DEBUG:
        print "Nodes No. = %d" % att_tree['*'].support
    treefile.close()
    return att_tree

def write_reorder_anonymized_data(filename,k):
    csvdata = pd.read_csv(filename,sep=';',header=0)
    csvdata = csvdata[ATT_NAMES]
    csvdata.to_csv('data/anonymized_'+str(k)+'_ordered.csv',sep=';',index=0)
