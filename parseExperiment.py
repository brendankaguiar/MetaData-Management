#from asyncio import DatagramTransport
import sys #module used to pass filename argument 
#import re #regular expression module to match desired chars
#import os #used to make directory for databases
def loadDatabase(fname):
  dBfile = open(fname, "r")
  all = dBfile.readlines()
  new = ""
  #index = 0
  for obj in all:
    #print(obj)
    new = new + obj
  new = new.strip()
  print(new)
  allLines = new.split(';')
  #print(allLines)
if len(sys.argv) < 2: #check arguments
     print("SQLB: ", end='')#no file

     #working on input looping function
else:
    loadDatabase(sys.argv[1])#load sql file
