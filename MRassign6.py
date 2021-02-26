# -*- coding: utf-8 -*-
"""
Created on Sun Oct 13 00:33:21 2019

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
    #  key=(i,k) i=1,...,L a,k=1,...,N
    #  list of matrix,i,j,value  A order of LxM,  j,k,value B of order MxN
    
    
    matrixrecord = record[0:4]
    L=5
    M=5
    N=5
    
    if matrixrecord[0]=="a":
     for i in range(0,N) :
         for j in range(0,M) :
           for k in range(0,N):             
               if matrixrecord[1]==i and matrixrecord[2]==j:
                  mr.emit_intermediate((i,k),("a",j,matrixrecord[3]))
    if matrixrecord[0]=="b":
        for j in range(0,N) :
          for k in range(0,L):
            for i in range(0,L):
              if matrixrecord[1]==j and matrixrecord[2]==k:
                 mr.emit_intermediate((i,k),("b",j,matrixrecord[3]))
def reducer(key,listofrecords):
    #sort values begining with "a" by j in a list
    M=5
    L=5
    N=5
    i=(0,L)
    k=(0,N)
    listM=[]
    listN=[]

    for item in listofrecords:
        if "a" in item:
               listM.append(item)
   # mr.emit((listM))\
    print(len(listM))
    print(key,listM)

    #for j in range(0,M):
        #  print(key,listM[j][2])
    for item in listofrecords:
        if "b" in item:
               listN.append(item)
    print(len(listN))
    print(key,listN)
    #for key in (i,k):
     # for j in range(0,M):
      #   if not listM[j][2]:
      #      pass 
      #   else:        
       #    print(key,listM[j][2]) 
       #  if not listN[j][2]:
       #     pass 
       #  else: 
       #    print(key,listN[j][2])
    for key in (i,k):
        for j in range(len(listN)):
          if  listM[j][2] is None or listN[j][2] is None:
             pass
          else:
              product=sum(listM[j][2]*listN[j][2])
              mr.emit((key,product))

# Do not modify below this line
# =============================
inputdata = open("C:/Users/hp/.spyder-py3/data/matrix.json","r")
mr.execute(inputdata, mapper, reducer)