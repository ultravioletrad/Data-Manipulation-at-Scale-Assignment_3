# -*- coding: utf-8 -*-
"""
Created on Fri Oct 11 20:06:29 2019

@author: hp
"""

import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    #  key=seq_id at record1
    #  nucleotide string  at record 2
    
    seq_id = record[0]
    listofstring = record[1]
    
    mr.emit_intermediate(seq_id,listofstring)
    
def reducer(seq_id,listofstrings):
    
    #for friend in listoffriends:
        #if listoffriends.count(friend)!=2:
           mr.emit((seq_id,listofstrings))

# Do not modify below this line
# =============================
inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)