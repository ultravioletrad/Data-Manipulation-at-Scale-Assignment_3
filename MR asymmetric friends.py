# -*- coding: utf-8 -*-
"""
Created on Fri Oct  4 18:57:19 2019

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
    #  key=person at record 1
    #  friend  at record 2
    #generate (person,friend) pairs at mapper
    key = record[0]
    friend = record[1]
    
    mr.emit_intermediate(key,friend)
    mr.emit_intermediate(friend,key)

def reducer(key,listoffriends):
    #key: person
    # value: list of friends)
    for friend in listoffriends:
        if listoffriends.count(friend)!=2:
           mr.emit((key,friend))

# Do not modify below this line
# =============================
inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)