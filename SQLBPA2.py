from asyncio import DatagramTransport
import sys #module used to pass filename argument 
import re #regular expression module to match desired chars
import os #used to make directory for databases
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

    def insertTableData(self,tblVals):
        tblVals.reverse()
        title = tblVals.pop()#remove title
        tblVals.reverse()
        i = self.getIndexofTable(title)
        self.Tbln[i].addTableData(tblVals)
        print("1 new record inserted.")

    def modifyTableData(self,tblVals):
        tblVals.reverse()
        i = self.getIndexofTable(tblVals.pop())
        self.Tbln[i].modTableData(tblVals)

    def modifyTableSchema(self,tblVals):#not used in PA2
        i = self.getIndexofTable(tblVals[0])
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
        i = self.getIndexofTable(tblName)
        del self.Tbln[i]
        print("Table", tblName, "deleted.")
        
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
        tblVals.pop(0)#remove table name from values
        self.Tbln[i].selectTableData(tblVals)

    def deleteTableData(self, tblVals):
        tblVals.reverse()
        title = tblVals.pop()#remove title
        tblVals.reverse()
        i = self.getIndexofTable(title)
        self.Tbln[i].delTableData(tblVals)

    class Table:
        def __init__(self, title):#default constructor
            self.title = title
            self.attr = []
            self.type = []
            self.len = []
            self.values = [[],[],[]]#list for [0]attr, [1]type, and [2]len
        def delTableData(self,tblVals):
            recordIndices = []
            i = 0#index for data in list to delete
            for lst in self.values[0]:
                for obj in lst:
                    if list(obj.values())[0] == tblVals[0]:#find attribute schema condition 
                        if tblVals[1] == '=':#find operator condition
                            if list(obj.keys())[0] == tblVals[2]:#find attribute data condition
                                recordIndices.append(i)#append record to delete
                        elif tblVals[1] == '>':
                            if tblVals[2].isnumeric():#check for numeric comparison
                                data = int(float(list(obj.keys())[0]) * 100)#make value comparable
                                data2 = int(tblVals[2]) * 100
                                if data > data2:#find attribute data condition
                                    recordIndices.append(i)#append record to delete
                i = i + 1
            recordIndices.reverse()
            k = 0#delete count
            for j in recordIndices:
                del self.values[0][j]#delete attribute data
                del self.values[1][j]#type data
                del self.values[2][j]#length data
                k = k + 1
            if k == 1:
                print(k, "record deleted.")
            else:
                print(k, "records deleted.")
        def selectTableData(self, tblVals):
            #print(tblVals)
            k = 0#index to clean up attribute schema
            for vals in tblVals[0]:
                tblVals[0][k] = vals.replace(' ','')#clean up schema
                k = k + 1
            i = len(tblVals[0])#get attribute count
            j = 1#index for newline tracking
            for obj in self.values[0]:
                for dictionary in obj:#dictionary of {data : schema}
                    if len(tblVals) > 1 and list(dictionary.values())[0] == tblVals[1][0]:#find matching schema condition
                        if tblVals[1][1]== "!=": #find matching operator
                            if tblVals[1][2] != list(dictionary.keys())[0]:
                                for dictionary2 in obj:#grab self.values for new search
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
            for dictionary in tables.values[0][0]:#write attribute schema
                schema = schema + list(dictionary.values())[0] + '|'
            tblFile = open(obj.name + '/' + tables.title + ".txt", "w")
            tblFile.write(schema + '\n')#write schema
            i = 1#index to prevent extra newline print
            for tableData in tables.values[0]:#write attribute data
                data = '|'
                for dictionary in tableData:
                    data = data + list(dictionary.keys())[0] + '|'
                if i < len(tables.values[0]):
                    tblFile.write(data + '\n')#write data
                else:
                    tblFile.write(data) 
                i = i + 1   
    print("All done.")    

def processDeleteKey(line):
    tblVals = []
    global dBn
    line = line.replace("delete from ",'')
    tblVals.append(line.split('\n')[0])#append table name
    line = line.split('\n')
    line[1] = line[1].replace("where ", '')
    tblVals.append(line[1].split(' ')[0])#append attribute schema condition
    tblVals.append(line[1].split(' ')[1])#append operator condition
    line[1] = line[1].split(' ')[2]
    tblVals.append(line[1].split(';')[0])#append data condition
    i = getIndexOfDatabase()
    tblVals[0] = tblVals[0].strip(' ')#remove space
    tblVals[0] = tblVals[0].capitalize()
    tblVals[3] = tblVals[3].strip("'")#remove single quotes    
    dBn[i].deleteTableData(tblVals)

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
    global dBn, error5
    tblVals = []
    if line.split(" ")[1] == '*':
        line = line.replace("SELECT * FROM ",'')
        line = line.replace("select * from ",'')
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
            if curLine.split(" ")[1] != '*':
                curLine = curLine + dBfile.readline() + dBfile.readline()
            processSelectKey(curLine)
        elif curLine.startswith("insert into"):
            processInsertKey(curLine)
        elif curLine.startswith("update"):
            curLine = curLine + dBfile.readline() + dBfile.readline()
            processUpdateKey(curLine)
        elif curLine.startswith("delete from "):
            curLine = curLine + dBfile.readline() + dBfile.readline()
            processDeleteKey(curLine)
        elif curLine.startswith(".EXIT") or curLine.startswith(".exit"):
            processExitKey()
        elif curLine.startswith('\n'):
            continue
        else:
            break
    dBfile.close()

#main program
if len(sys.argv) < 2: #check arguments
     print("SQLB: ", end='')#no file

     #working on input looping function
else:
    loadDatabase(sys.argv[1])#load sql file
