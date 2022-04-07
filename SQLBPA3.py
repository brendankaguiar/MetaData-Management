import sys #module used to pass filename argument 
import re #regular expression module for mulitple cases of string matching
#import os #used to make directory for databases
dBn = [] #list to contain databases]
error1 = 0 #CREATE DATABASE error
error2 = 0 #DROP DATABASE error
error3 = 0 #CREATE TABLE error
error4 = 0 #DROP TABLE error
error5 = 0 #SELECT TABLE error
inUse = [] #current database in use

class DataBase:
    def __init__(self,name):
        self.name = name
        self.Tbln = []
        print("Database", name, "created.")

    def getIndexofTable(self, target):#helper function to get table to use
        i = 0
        for obj in self.Tbln:
            if obj.title == target:
                break#get index of table
            i = i + 1
        return i

    def insertTableRecord(self,tblVals):
        tblVals.reverse()
        title = tblVals.pop()#remove title
        tblVals.reverse()
        i = self.getIndexofTable(title)
        self.Tbln[i].addTableRecord(tblVals)
        print("1 new record inserted.")

    def insertTableSchema(self,tblVals):
        global error3
        if self.Tbln == []:
            title = tblVals[0]
            obj = self.Table(title)
            obj.setTableSchema(tblVals)
            self.Tbln.append(obj)#append to empty table list
            print("Table", title, "created.")
        else:
            for obj in self.Tbln:
                if obj.title == tblVals[0]:
                    print("!Failed to create", obj.title, "because it already exists.")
                    error3 = 1
            if error3 == 1:
                error3 = 0#reset
            else:
                title = tblVals[0]
                obj = self.Table(title)
                obj.setTableSchema(tblVals)
                self.Tbln.append(obj)#append to non-empty table list
                print("Table", title, "created.")
    
    def selectTableSchema(self,tblVals):
        i = self.getIndexofTable(tblVals[0])
        j = 0
        if len(tblVals) < 2:
            tblVals.append(self.Tbln[i].attr)#append all attributes schema to show
        for attr in tblVals[1]:#show attribute schema
            while j < len(self.Tbln[i].attr):
                if self.Tbln[i].attr[j] == attr.replace(' ',''):
                    if self.Tbln[i].len[j] != 0:
                        print(self.Tbln[i].attr[j], self.Tbln[i].type[j], end='')
                        print('(' + str(self.Tbln[i].len[j]) +')', end='')
                    else:
                        print(self.Tbln[i].attr[j], self.Tbln[i].type[j], end='')
                    if j + 1 == len(self.Tbln[i].attr):
                        print(' ', end='\n')
                    else :
                        print(' | ', end='')
                j = j + 1
            j = 0
        tblVals.pop(0)#remove table name from tuples
        self.Tbln[i].selectTableRecord(tblVals)

    class Table:
        def __init__(self, title):#default constructor
            self.title = title
            self.attr = []
            self.type = []
            self.len = []
            self.tuples = [[],[],[]]#list for [0]attr, [1]type, and [2]len

        def selectTableRecord(self, tblVals):
            k = 0#index to clean up attribute schema
            for vals in tblVals[0]:
                tblVals[0][k] = vals.replace(' ','')#clean up schema
                k = k + 1
            i = len(tblVals[0])#get attribute count
            j = 1#index for newline tracking
            for obj in self.tuples[0]:
                for dictionary in obj:#dictionary of {data : schema}
                    if len(tblVals) > 1 and list(dictionary.values())[0] == tblVals[1][0]:#find matching schema condition
                        if tblVals[1][1]== "!=": #find matching operator
                            if tblVals[1][2] != list(dictionary.keys())[0]:
                                for dictionary2 in obj:#grab self.tuples for new search
                                    if list(dictionary2.values())[0] in tblVals[0]:#find data for schema in use
                                        key = list(dictionary2.keys())                
                                        if j < i:
                                            print(key.pop() + '|', end='')
                                            j = j + 1
                                        else:
                                            print(key.pop())
                                            j = 1#reset iterator for next line
            if len(tblVals) == 1:#print data for select *
                i = len(self.attr)#get attribute count
                j = 1#new line tracking iterator
                for obj in self.tuples[0]:
                    for vals in obj:
                        key = list(vals.keys())
                        if j < i:
                            print(key.pop() + '|', end='')
                            j = j + 1
                        else:
                            print(key.pop())
                            j = 1#reset iterator for next line
                                        
        def setTableSchema(self, vals):
            vals.reverse()  
            self.title = vals.pop()
            while vals != []:
                self.attr.append(vals.pop())
                self.type.append(vals.pop())
                self.len.append(vals.pop())
        def addTableRecord(self, vals):
            i = 0#attribute counter
            AttrData = []
            TypeData = []
            LenData = []
            for obj in vals:
                AttrData.append({obj : self.attr[i]})#append attribute data
                TypeData.append({self.type[i] : self.attr[i]})#append type data
                LenData.append({self.len[i] : self.attr[i]})#append length data
                i = i + 1
            #append record
            self.tuples[0].append(AttrData)
            self.tuples[1].append(TypeData)
            self.tuples[2].append(LenData)          

def getIndexOfDatabase():#helper function
    i = 0
    global inUse, dBn
    for obj in dBn:
        if dBn[i].name == inUse:
            break
        else:
            i = i + 1#get index of database in use
    return i


def processCreateKey(line):
    line = line.replace(re.findall(r'create ',line, flags=re.IGNORECASE)[0],'')#return matching string and remove
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
        line = line.replace(re.findall(r'table ',line, flags=re.IGNORECASE)[0], '')
        tblVals.append(line.split("(")[0])#append table name
        line = line.split("(")[1]
        line = line.split(");")[0]
        line = line.split(",")#append items to list
        for obj in line:
            if re.findall('[0-9]+', obj) == []:#if int or float
                obj = obj.split(' ')
                if len(obj) == 3:
                    obj.remove('')
                obj.reverse()
                tblVals.append(obj.pop())#add attribute
                tblVals.append(obj.pop())#add type
                tblVals.append(0)#add len
            else:
                if obj.find("var"):
                    obj = obj.split(' ')
                    obj.reverse()
                    if len(obj) == 3:
                        obj.remove('')
                    tblVals.append(obj.pop())#add attribute
                    tblVals.append("varchar")#add type
                    length = re.findall('[0-9]+', obj.pop())
                    tblVals.append(length.pop())#add len
                else:    
                    obj = obj.split(' ')
                    obj.reverse()
                    obj = obj.split(' ')
                    obj.reverse()
                    if len(obj) == 3:
                        obj.remove('')
                    length = re.findall('[0-9]+', obj)
                    tblVals.append(obj.pop())#add attribute
                    tblVals.append("char")#add type
                    tblVals.append(length.pop())#add len
        i = getIndexOfDatabase()
        dBn[i].insertTableSchema(tblVals)
    
def processUseKey(line):
    line = line.replace("USE ",'')
    temp = line.split(';')[0]#assign dBname
    print("Using database " + temp + ".")
    return temp

def processInsertKey(line):
    global dBn
    tblVals = []
    line = line.replace("insert into ", '')
    tblName = line.split("(")[0]
    tblVals.append(tblName.split(" ")[0])#add table title
    tblAttr = line.split("(")[1]
    tblAttr = tblAttr.split(");")[0]
    tblAttr = tblAttr.split(",", -1)
    for obj in tblAttr:
        attr = re.findall('\S', obj, )#eliminate everything except digits, [a-z], and [A-Z]
        attr = ''.join(map(str,attr))#map attrlist to string
        attr = attr.replace("'","")#remove any single quotes
        tblVals.append(attr)
    i = getIndexOfDatabase()
    dBn[i].insertTableRecord(tblVals)

def processSelectKey(line):
    global dBn, error5
    tblVals = []
    line = line.split("\n")
    if line[0].endswith("* "):#select all
        tblVals.append('*')#add projection
    if line[1].find("inner join") > 0:
        tblVals.append("inner join")#add join type
    elif line[1].find("left outer join") > 0:
        tblVals.append("left outer join")
    else:
        tblVals.append("natural join")
    tblVars = line[1].split(' ')
    tblVars[2] = tblVars[2].replace(',', '')
    tblVals.append(tblVars[2])#append table variable name
    tblVals.append(tblVars[1])#append table name
    tblVars.pop()
    tblVals.append(tblVars.pop())#append table variable name
    tblVals.append(tblVars.pop())#append tabl name
    schemaVars = line[2].split(' ')
    tblVals.append(schemaVars[1].split(".")[1])#add table1 col.
    tblVals.append(schemaVars[3].split(".")[1])#add table2 col.
    tblVals.append(schemaVars[2])#add condition operator
    print(tblVals)
    i = getIndexOfDatabase()
    if dBn[i].Tbln == []:
        error5 = 1#empty database
    join1, join2
    for tableObj in dBn[i].Tbln:
        if tableObj.title == tblVals[3]:
            join1 = tableObj.tuples
        elif tableObj.title == tblVals[5]:
            join2 = tableObj.tuples
    #begin selecting join types showing data. 0 down, 3 to go
    """
        tblVals.append(line.split(';')[0])#add table name
    else:
        line = line.split(';')[0]
        line = line.split('\n')
        name = line[1].replace("from ",'').capitalize()
        tblVals.append(str(name.replace(' ', '')))#append table name
        line[0] = line[0].replace("select ",'')
        tblVals.append(line[0].split(','))#append attribute schema to show
        line[2] = line[2].replace("where ", '')
        tblVals.append(line[2].split(" "))#append condition data
    i = getIndexOfDatabase()
    if dBn[i].Tbln == []:
        error5 = 1#empty database
    for tableObj in dBn[i].Tbln:
        if tableObj.title == tblVals[0]:
            dBn[i].selectTableSchema(tblVals)
            error5 = 0
            break
        else:
            error5 = 1
    if error5 == 1:
        print("!Failed to query table", tblVals[0], "because it does not exist.")
        error5 = 0  

"""
def parselines(curLine):
    global inUse
    if re.findall(r'create',curLine, flags=re.IGNORECASE):#ignores case sensitivity
        processCreateKey(curLine) 
    elif curLine.startswith("USE"):
        inUse = processUseKey(curLine)
    elif curLine.startswith("insert into"):
        processInsertKey(curLine)
    elif re.findall(r'select',curLine, flags=re.IGNORECASE):
        processSelectKey(curLine)
    elif re.findall(r'.exit',curLine, flags=re.IGNORECASE):
        #processExitKey()
        print(curLine)
def loadDatabase(fname):
    dBfile = open(fname, "r")
    commands = dBfile.readlines()
    i = 0
    linesToDelete = []
    for line in commands:
        if line.startswith("--") or line.startswith("\n"):#clear comments, empty lines
            linesToDelete.append(i)#append index of line to pop
        i = i + 1
    linesToDelete.reverse()
    for j in linesToDelete:#reverse lines to pop
        commands.pop(j)#remove unwanted lines
    extraLine = ""
    for line in commands:
        if line.endswith(";\n") is True or line.startswith(".") is True:
            line = extraLine + line
            parselines(line)
            extraLine = ""
        else:
            extraLine = extraLine + line#append line without semicolon for next iteration
  
#main program
if len(sys.argv) < 2: #check arguments
     print("SQLB: ", end='')#no file

     #working on input looping function
else:
    loadDatabase(sys.argv[1])#load sql file
