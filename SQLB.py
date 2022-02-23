from asyncio.windows_events import NULL
from contextlib import nullcontext
from logging import NullHandler
import sys #module used to pass filename argument 

dBn = [] #list to contain databases]
error1 = 0 #CREATE error
error2 = 0 #DROP error
inUse = NULL
class Table:#table class
    def __init__(self,title,len,attr=None,type=None):#default constructor
        if attr is None:
            attr = []
        if type is None:
            type = []
        self.title = title
        self.len = len

class DataBase:#Database class
    def __init__(self,name):
        self.obj1 = Table(NULL,0)#Database has a Table
        self.name = name
        print("Database", name, "created.")
def processUseKey(line):
    line = line.replace("USE ",'')
    temp = line.split(';')[0]#assign dBname
    print("Using database ", temp, ".")
    return temp

def processDropKey(line): 
    line = line.replace("DROP ",'')
    if line.startswith("DATABASE "):
        line = line.replace("DATABASE ", '')
        global dBn, error2
        temp = line.split(';')[0]
        error2 = 1
        for obj in dBn:
            if obj.name == temp:
                print("Database",obj.name,"deleted.")
                dBn.pop()
                error2 = 0
        if error2 == 1:
            print("!Failed to delete database", obj.name, "because it does not exist.")
            error2 = 0
    else:
        line = line.replace("TABLE ", '')

def processCreateKey(line):
    line = line.replace("CREATE ",'')
    if line.startswith("DATABASE "):
        line = line.replace("DATABASE ", '')
        global dBn
        if dBn == []:
            dBn.append(DataBase(line.split(';')[0]))#append to empty list
        else:
            temp = line.split(';')[0]
            global error1
            for obj in dBn:
                if obj.name == temp:
                    print("!Failed to create database", obj.name, "because it already exists.")
                    error1 = 1
            if error1 == 1:
                error1 = 0#reset
            else:
                dBn.append(DataBase(line.split(';')[0]))#append to non-empty list
    else:
        line = line.replace("TABLE ", '')

def loadDatabase(fname):
    dBfile = open(fname, "r")
    curLine = dBfile.readline()
    global inUse
    while curLine:
        curLine = dBfile.readline()
        if curLine.startswith("CREATE "):
            processCreateKey(curLine)
        elif curLine.startswith("DROP "):
            processDropKey(curLine)
        elif curLine.startswith("USE "):
            inUse = processUseKey(curLine)
    dBfile.close()

if len(sys.argv) < 2: #check arguments
     print("calling function")#no argument
else:
    loadDatabase(sys.argv[1])#load sql file
