# -*- coding: utf-8 -*-
"""
Created on Mon Sep 30 20:10:30 2019

@author: hp
"""

import MapReduce
import sys
#part 1 global var 
mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line
 
 #part 2 Mapper function
 
def mapper(record):
     #key=document_id
     #value=text
   key=record[0]   
   value=record[1]
   words=value.split()
   for w in words:
       mr.emit_intermediate(w,key)
       
 #part 3 Reducer function      
def reducer(w,list_of_docid):
    #key=word
    #value=document_id_list
    #removing duplicates from doc list
    dict_docid=list(dict.fromkeys(list_of_docid))
    mr.emit((w,dict_docid))
  
    #Part 4
# Do not modify below this line
# =============================
#if __name__ == '__main__': 
inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)
        
