from asyncio import DatagramTransport
import sys #module used to pass filename argument 
import re #regular expression module to match desired chars
dBn = [] #list to contain databases]
error1 = 0 #CREATE DATABASE error
error2 = 0 #DROP DATABASE error
error3 = 0 #CREATE TABLE error
error4 = 0 #DROP TABLE error
error5 = 0 #SELECT TABLE error
inUse = [] #current database in use
class DataBase:
    def __init__(self,name):
        #self.Table = self.Table([])#Database has a Table
        self.name = name
        self.Tbln = []
        print("Database", name, "created.")

    def insertTableData(self,tblVals):
        tblVals.reverse()
        title = tblVals.pop()#remove title
        tblVals.reverse()
        i = 0
        for obj in self.Tbln:
            if obj.title == title:
                break#get index of table to modify
            i = i + 1
        self.Tbln[i].addTableData(tblVals)
        print("1 new record inserted.")
    def modifyTableData(self,tblVals):
        tblVals.reverse()
        i = 0
        for obj in self.Tbln:
            if self.Tbln[i].title == tblVals.pop():
                break
            else:
                i = i + 1#get index of table in use
        self.Tbln[i].modTableData(tblVals)
    def modifyTableSchema(self,tblVals):#not used in PA2
        i = 0
        for obj in self.Tbln:
            if obj.title == tblVals[0]:
                break#get index of table to modify
            i = i + 1
        self.Tbln[i].modTableSchema(tblVals)

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

    def removeTable(self,tblName):#not used in PA2
        i = 0
        for obj in self.Tbln:
            if self.Tbln[i].title == tblName:
                break
            else:
                i = i + 1#get index of table in use
        del self.Tbln[i]
        print("Table", tblName, "deleted.")
        
    def selectTableSchema(self,tblName):
        i = 0
        for obj in self.Tbln:
            if self.Tbln[i].title == tblName:
                break
            else:
                i = i + 1#get index of table in use 
        j = 0
        while j < len(self.Tbln[i].attr):
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
        self.Tbln[i].selectTableData()
    class Table:
        def __init__(self, title):#default constructor
            self.title = title
            self.attr = []
            self.type = []
            self.len = []
            self.values = [[],[],[]]#list for [0]attr, [1]type, and [2]len
        def selectTableData(self):
            #print(self.values[0])
            i = len(self.attr)#get attribute count
            j = 1#new line tracking iterator
            for obj in self.values[0]:
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
        def modTableSchema(self, vals):#currently supports add schema only, not used in PA2
            vals.reverse()
            self.title = vals.pop()
            self.attr.append(vals.pop())
            self.type.append(vals.pop())
            self.len.append(vals.pop())
        def modTableData(self, Data):#[dataCond, schemaCond, newData, schema]
            #print(Data)
            #print(self.values[0])
            i = 0#index for attribute data
            recordIndices = []#index for attribute schema
            for obj in self.values[0]:
                for vals in obj:
                    if vals == {Data[0] : Data[1]}:
                        recordIndices.append(i)
                i = i + 1
            j = 0#counter for records
            for indices in recordIndices:
                for vals in self.values[0][indices]:
                    if list(vals.values())[0] == Data[3]:
                        #print("working")
                        l = self.attr.index(Data[3])#get index of schema to change
                        self.values[0][indices][l] = {Data[2] : Data[3]}
                        j = j + 1
                        #set attribute data
            if j == 0 or j > 1:
                print(j, "records modified.")
            else:
                print(j, "record modified.")          
        def addTableData(self, vals):
            i = 0#attribute counter
            AttrData = []
            TypeData = []
            LenData = []
            for obj in vals:
                AttrData.append({obj : self.attr[i]})
                TypeData.append({self.type[i] : self.attr[i]})
                LenData.append({self.len[i] : self.attr[i]})
                i = i + 1
            self.values[0].append(AttrData)
            self.values[1].append(TypeData)
            self.values[2].append(LenData)
            

def getIndexOfDatabase():#helper function
    i = 0
    global inUse
    for obj in dBn:
        if dBn[i].name == inUse:
            break
        else:
            i = i + 1#get index of database in use
    return i
def processExitKey():
    print("All Done.")

def processUpdateKey(line):
    tblVals = []
    global dBn
    line = line.split(';')[0]
    line = line.split('\n')
    line[0] = line[0].replace("update ",'')
    tblVals.append(line[0].split(' ')[0])#add table name
    line[1] = line[1].replace("set ", '')
    tblVals.append(line[1].split(' ')[0])#add attribute schema to modify
    line[1] = line[1].split(' ')[2]#parse attribute data to modify
    tblVals.append(line[1].replace("'",''))#remove single quotes and append data
    line[2] = line[2].replace("where ", '')
    line[2] = line[2].replace("where " + tblVals[1] + " = ",'')#parse new attribute data
    tblVals.append(line[2].split(' ')[0])#parse attribute schema that is conditioned upon
    line[2]= line[2].replace("'", '')
    tblVals.append(line[2].split(' ')[2])#parse attribute data that is conditioned upon
    i = getIndexOfDatabase()
    dBn[i].modifyTableData(tblVals)

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
    dBn[i].insertTableData(tblVals)

def processSelectKey(line):
    line = line.replace("SELECT * FROM ",'')
    line = line.replace("select * from ",'')
    global dBn, error5
    temp = line.split(';')[0]
    i = getIndexOfDatabase()
    if dBn[i].Tbln == []:
        error5 = 1#empty database
    for tableObj in dBn[i].Tbln:
        if tableObj.title == temp:
            dBn[i].selectTableSchema(temp)
            error5 = 0
            break
        else:
            error5 = 1
    if error5 == 1:
        print("!Failed to query table", temp, "because it does not exist.")
        error5 = 0  

def processUseKey(line):
    line = line.replace("USE ",'')
    temp = line.split(';')[0]#assign dBname
    print("Using database " + temp + ".")
    return temp

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
        
def loadDatabase(fname):
    dBfile = open(fname, "r")
    curLine = dBfile.readline()
    global inUse
    while curLine:
        curLine = dBfile.readline()
        if curLine.startswith("CREATE"):
            processCreateKey(curLine)
        elif curLine.startswith("USE"):
            inUse = processUseKey(curLine)
        elif curLine.startswith("select") or curLine.startswith("SELECT"):
            processSelectKey(curLine)
        elif curLine.startswith("insert into"):
            processInsertKey(curLine)
        elif curLine.startswith("update"):
            curLine = curLine + dBfile.readline() + dBfile.readline()
            processUpdateKey(curLine)
        elif curLine.startswith(".EXIT"):
            processExitKey()
        elif curLine.startswith('\n'):
            continue
        else:
            break
    dBfile.close()

if len(sys.argv) < 2: #check arguments
     print("calling function")#no argument
else:
    loadDatabase(sys.argv[1])#load sql file
