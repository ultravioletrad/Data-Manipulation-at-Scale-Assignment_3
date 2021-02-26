# -*- coding: utf-8 -*-
"""
Created on Wed Oct  2 19:46:55 2019

@author: hp
"""

import MapReduce
import sys
 


#Part 1 global var.
mr=MapReduce.MapReduce()
 
 #part 2 Mapper function
def mapper(record):
     #key= the join attribute,i.e. "order-id" at record 1
     #value=whole of the tuple including the identifier "LineItem","Order"
     #17 attributes for LineItem,10 for Order
   key=record[1]   
   value=record[0:26]
   
   mr.emit_intermediate(key,value)
       
 #part 3 Reducer function      
def reducer(key,list_of_values):
      for value in list_of_values:
         for key in value:
              mr.emit(list_of_values)
  
    #Part 4
# Do not modify below this line
# =============================
#if __name__ == '__main__': 
inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)