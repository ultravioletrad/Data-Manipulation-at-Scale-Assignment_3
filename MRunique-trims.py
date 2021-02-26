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
    strings = record[1]
    strings2=strings[:-10]
    string=strings2.split()
    for s in string:
      mr.emit_intermediate(s,1)
    
def reducer(string,listofcounts):
      mr.emit((string))
     

# Do not modify below this line
# =============================
inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)