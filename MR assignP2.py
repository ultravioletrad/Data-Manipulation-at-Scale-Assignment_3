# -*- coding: utf-8 -*-
"""
Created on Wed Oct  2 00:31:46 2019

@author: hp
"""

import MapReduce
#import sys
 


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
def reducer(key,listofvalues):
      for x in listofvalues:
         for y in listofvalues:
             if len(x)<len(y):
               if x[0]!=y[0]:
                 if key in x:
                   if key in y:
                     combxy=(x+y)
                     mr.emit((combxy)) 
  
    #Part 4
# Do not modify below this line
# =============================
#if __name__ == '__main__': 
inputdata=open("C:/Users/hp/.spyder-py3/data/records.json","r")  
  #inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)