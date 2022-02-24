from lib2to3.pgen2.token import NEWLINE
import sys #module used to pass filename argument 

dBn = [] #list to contain databases]
error1 = 0 #CREATE DATABASE error
error2 = 0 #DROP DATABASE error
error3 = 0 #CREATE TABLE error
error4 = 0 #DROP TABLE error
inUse = [] #
class Table:#table class
    def __init__(self,title,len=None,attr=None,type=None):#default constructor
        if len is None:
            self.len = []
        if attr is None:
            self.attr = []#list for multiple attributes & their types
        if type is None:
            self.type = []
        self.title = title
    def setTable(self, vals):
        vals.reverse()
        self.title = vals.pop()
        while vals != []:
            self.attr.append(vals.pop())
            self.type.append(vals.pop())
            self.len.append(vals.pop())
        print("Table",self.title, "created.")
        return self
    def getAttrCount(self):
        return len(self.attr)

class DataBase:#Database class
    def __init__(self,name):
        self.obj1 = Table([])#Database has a Table
        self.name = name
        self.Tbln = []
        print("Database", name, "created.")
    def setValues(self,tblVals):
        global error3
        if self.Tbln == []:
            self.Tbln.append(self.obj1.setTable(tblVals))#append to empty table list
        else:
            for obj in self.Tbln:
                tblVals.reverse()
                if obj.title == tblVals.pop():
                    print("!Failed to create ", obj.title, "because it already exists.")
                    error3 = 1
            if error3 == 1:
                error3 = 0#reset
            else:
                self.Tbln.append(self.obj1.setTable(tblVals))#append to non-empty table list
    def removeTable(self,tblName):
        i = 0
        for obj in self.Tbln:
            if self.Tbln[i].title == tblName:
                break
            else:
                i = i + 1#get index of table in use
        self.Tbln.pop(i)
        print(len(self.obj1.attr))
        print("Table", tblName, "deleted.")
        
    def selectTable(self,tblName):
        i = 0
        for obj in self.Tbln:
            if dBn[i].name == inUse:
                break
            else:
                i = i + 1#get index of table in use
        j = self.Tbln[i].getAttrCount()
        for obj2 in self.Tbln[i].attr:
            if self.Tbln[i].len[j] != 0:#if using varchar or char
                print(self.Tbln[i].attr[j], self.Tbln[i].type[j], end='')
                print('(' + str(self.Tbln[i].len[j]) +')', end='')
            else:
                print(self.Tbln[i].attr[j], self.Tbln[i].type[j], end='')
            print(' | ', end='')
            j = j + 1
            print(j)
        
def processSelectKey(line):
    line = line.replace("SELECT * FROM ",'')
    global dBn
    temp = line.split(';')[0]
    i = 0
    for obj in dBn:
        if dBn[i].name == inUse:
            break
        else:
            i = i + 1#get index of database in use
    for tableObj in dBn[i].Tbln:
                if tableObj.title == temp:
                    dBn[i].selectTable(temp)        

def processUseKey(line):
    line = line.replace("USE ",'')
    temp = line.split(';')[0]#assign dBname
    print("Using database " + temp + ".")
    return temp

def processDropKey(line): 
    line = line.replace("DROP ",'')
    global dBn, error2, error4
    if line.startswith("DATABASE "):
        line = line.replace("DATABASE ", '')
        temp = line.split(';')[0]
        error2 = 1
        for obj in dBn:
            if obj.name == temp:
                print("Database",obj.name,"deleted.")
                dBn.pop()
                error2 = 0
        if error2 == 1:
            print("!Failed to delete database", temp, "because it does not exist.")
            error2 = 0
    else:
        line = line.replace("TABLE ", '')
        temp = line.split(';')[0]
        i = 0
        for obj in dBn:
            if dBn[i].name == inUse:
                break
            else:
                i = i + 1#get index of database in use
        if dBn[i].Tbln != []:
            for tableObj in dBn[i].Tbln:
                if tableObj.title == temp:
                    dBn[i].removeTable(temp)
                    break
                else:
                    error4 = 1
            if error4 == 1:
                error4 = 0#reset
        elif error4 == 1 or  dBn[i].Tbln == []:
            print("!Failed to delete", temp, "because it does not exist.")

def processCreateKey(line):
    line = line.replace("CREATE ",'')
    global dBn, error1
    if line.startswith("DATABASE "):
        line = line.replace("DATABASE ", '')
        if dBn == []:
            dBn.append(DataBase(line.split(';')[0]))#append to empty list
        else:
            temp = line.split(';')[0]
            for obj in dBn:
                if obj.name == temp:
                    error1 = 1
            if error1 == 1:
                error1 = 0#reset
                print("!Failed to create database", obj.name, "because it already exists.")
            else:
                dBn.append(DataBase(line.split(';')[0]))#append to non-empty list
    else:
        tblVals = []
        line = line.replace("TABLE ", '')
        tblName = line.split(" (")[0]
        line = line.split(" (")[1]
        tblVals.append(tblName)#add title
        escLoop = 0
        while escLoop == 0:#iterates through each attr. until ';' found
            if line.find(',') != -1:
                attr = line.split(',')[0]
                line = line.split(", ")[1]#parse line for next iteration
            else:
                escLoop = 1
                attr = line.split(");")[0]
            tblVals.append(attr.split(' ')[0]) #add attribute
            type = attr.split(' ')[1]#parse type
            if type.find("char") != -1:
                tblVals.append(type.split('(')[0])#add type for varchar or char
                type = type.split('(')[1]
                len = type.split(')')[0]
                tblVals.append(int(len))#add len
            else:
                tblVals.append(type)#add type for int or float
                tblVals.append(0)#set len
        #push values to table in database
        i = 0
        for obj in dBn:
            if dBn[i].name == inUse:
                break
            else:
                i = i + 1#get index of database in use
        dBn[i].setValues(tblVals)
       # print(tblVals)

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
        elif curLine.startswith("SELECT"):
            processSelectKey(curLine)
    dBfile.close()

if len(sys.argv) < 2: #check arguments
     print("calling function")#no argument
else:
    loadDatabase(sys.argv[1])#load sql file
