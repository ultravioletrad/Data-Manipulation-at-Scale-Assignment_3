# -*- coding: utf-8 -*-
"""
Created on Thu Oct  3 16:55:11 2019

@author: hp
"""

import MapReduce
#import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    #  key=person at record 1
    #  friend  at record 2
    #generate (person-name,friend-count) pairs at mapper
    key = record[0]
    friend = record[1]
    
    mr.emit_intermediate(key, 1)

def reducer(key,listofcounts):
    # key: person
    # value: list of friend counts
    for person in key:
     friendcount=sum(listofcounts)
 
    mr.emit((key,friendcount))

# Do not modify below this line
# =============================
inputdata = open("C:/Users/hp/.spyder-py3/data/friends.json","r")
mr.execute(inputdata, mapper, reducer)