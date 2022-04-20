import sys #module used to pass filename argument 
import os #used to make directory for databases
import re
from tracemalloc import start #regular expression module for mulitple cases of string matching
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
        #[select values, join type, L. Table Var, L. Table name, R. Table Var, R. Table name, table1 col, table2 col, cond., R. tuples]
        i = self.getIndexofTable(tblVals[3])#left table index
        r = self.getIndexofTable(tblVals[5])#right table index
        schema = []
        if tblVals[0] == '*':
            schema.append(self.Tbln[i].attr)#append all attributes schema to show
            schema[-1] = schema[-1] + self.Tbln[r].attr
            schema.append(self.Tbln[i].type)#append all types schema to show
            schema[-1] = schema[-1] + self.Tbln[r].type
            schema.append(self.Tbln[i].len)#append all types schema to show
            schema[-1] = schema[-1] + self.Tbln[r].len
        j = 0
        for attr in schema[0]:#show attribute schema
            if schema[2][j] != 0:
                print(attr, schema[1][j], end='')
                print('(' + schema[2][j] + ')', end='')
            else:
                print(attr,  schema[1][j], end='')
            if j + 1 == len(schema[0]):
                print(' ', end='\n')
            else:
                print('|', end='')
            j = j + 1
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
            tblRef = ""
            for tuplesL in self.tuples[0]:
                for dict in tuplesL:
                    if list(dict.values())[0] == tblVals[5]:#find schema to match for Left tuple
                        schemaL = list(dict.keys())[0]#schema data to match
                for tuplesR in tblVals[8][0]:
                    for dict in tuplesR:
                        if list(dict.values())[0] == tblVals[6]:#find matching schema for Right Tuple
                            if list(dict.keys())[0] == schemaL:#find matching data
                                i = len(list(tuplesR))
                                
                                for dict2 in tuplesL:
                                    print(list(dict2.keys())[0]+ '|', end='')
                                    tblRef = list(dict2.keys())[0]#get reference for start of next selection


                                for dict3 in tuplesR:
                                    if i > 1:
                                        print(list(dict3.keys())[0] + '|', end='')
                                        i = i - 1
                                    else:
                                        print(list(dict3.keys())[0])
            if tblVals[0] == "left outer join":
                start_found = 0
                for val in self.tuples[0]:
                    i = len(val)
                    for dict in val:
                        if start_found == 1:
                            print(list(dict.keys())[0]+ '|', end='')
                            i = i - 1
                        elif tblRef == list(dict.keys())[0]:
                            start_found = 1  
                    if i == 0:
                        print("|")        

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

def processExitKey():
    global dBn
    for obj in dBn:
        os.mkdir(obj.name)#make directory
        for tables in obj.Tbln:
            schema = '|'
            for dictionary in tables.tuples[0][0]:#write attribute schema
                schema = schema + list(dictionary.values())[0] + '|'
            tblFile = open(obj.name + '/' + tables.title + ".txt", "w")
            tblFile.write(schema + '\n')#write schema
            i = 1#index to prevent extra newline print
            for tableData in tables.tuples[0]:#write attribute data
                data = '|'
                for dictionary in tableData:
                    data = data + list(dictionary.keys())[0] + '|'
                if i < len(tables.tuples[0]):
                    tblFile.write(data + '\n')#write data
                else:
                    tblFile.write(data) 
                i = i + 1   
    print("All done.") 

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
        varLength = []
        if line.find("varchar") > 0:
            varLength = re.findall(r'-?\d+\.?\d*', line)#get length of varchar
        line = line.split("(")[1]
        line = line.split(");")[0]
        line = line.split(",")#append items to list
        
        for obj in line:
            if obj.find('int') > 0:#if int or float
                obj = obj.split(' ')
                if len(obj) == 3:
                    obj.remove('')
                obj.reverse()
                tblVals.append(obj.pop())#add attribute
                tblVals.append(obj.pop())#add type
                tblVals.append(0)#add len
            else:
                if obj.find('var') > 0:#print if varchar
                    obj = obj.split(' ')
                    obj.reverse()
                    if len(obj) == 3:
                        obj.remove('')
                    tblVals.append(obj.pop())#add attribute
                    tblVals.append("varchar")#add type
                    tblVals.append(varLength.pop())#add len
                else:#print if char  
                    obj = obj.split(' ')
                    obj.reverse()
                    obj = obj.split(' ')
                    obj.reverse()
                    if len(obj) == 3:
                        obj.remove('')
                    tblVals.append(obj.pop())#add attribute
                    tblVals.append("char")#add type
                    tblVals.append(0)#add len
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
    tblVals = []#list values commented below
#[select values, join type, L. Table Var, L. Table name, R. Table Var, R. Table name, table1 col, table2 col, cond., R tuples]
    line = line.split("\n")
    if line[0].endswith("* "):#select all
        tblVals.append('*')#add projection
    if line[1].find("inner join") > 0:
        tblVals.append("inner join")#add join type
    elif line[1].find("left outer join") > 0:
        tblVals.append("left outer join")
    else:
        tblVals.append("inner join")
    tblVars = line[1].split(' ')
    tblVars[2] = tblVars[2].replace(',', '')
    tblVals.append(tblVars[2])#append table variable name
    tblVals.append(tblVars[1])#append table name
    tblVars.pop()
    tblVals.append(tblVars.pop())#append table variable name
    tblVals.append(tblVars.pop())#append tabl name
    schemaVars = line[2].split(' ')
    tblVals.append(schemaVars[1].split(".")[1])#add table1 col.
    schemaVars[3] = schemaVars[3].split(".")[1]#get column
    tblVals.append(schemaVars[3].split(";")[0])#add table2 col. removing ';'
    tblVals.append(schemaVars[2])#add condition operator
    i = getIndexOfDatabase()
    if dBn[i].Tbln == []:
        error5 = 1#empty database
    sourceR = []#right tuples source
    for tableObj in dBn[i].Tbln:
        if tableObj.title == tblVals[5]:#search for right table
            sourceR = tableObj.tuples
            break
    tblVals.append(sourceR)#append right table
    for tableObj in dBn[i].Tbln:
        if tableObj.title == tblVals[3]:#search for left table
            dBn[i].selectTableSchema(tblVals)
            error5 = 0
            break
        else:
            error5 = 1
    if error5 == 1:
        print("!Failed to query table", tblVals[0], "because it does not exist.")
        error5 = 0  
    #begin selecting join types showing data. 0 down, 3 to go

 
def parselines(curLine):
    global inUse
    if re.findall(r'create',curLine, flags=re.IGNORECASE):#ignores case sensitivity
        processCreateKey(curLine) 
    elif curLine.startswith("USE"):
        inUse = processUseKey(curLine)
    elif curLine.startswith("insert into"):
        processInsertKey(curLine)
    elif re.findall(r'select', curLine, flags=re.IGNORECASE):
        processSelectKey(curLine)
    elif re.findall(r'.exit', curLine, flags=re.IGNORECASE):
        processExitKey()

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
     #needs input looping function
else:
    loadDatabase(sys.argv[1])#load sql file
