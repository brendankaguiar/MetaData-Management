
import sys #module used to pass filename argument 

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
        
    def modifyValues(self,tblVals):
        modType = tblVals.pop(1)#used for different types of modification
        title = tblVals[0]
        if modType == "ADD":
            i = 0
            for obj in self.Tbln:
                if obj.title == tblVals[0]:
                    break#get index of table to modify
                i = 1 + 1
            self.Tbln[i].modTable(tblVals)
            print("Table", title, "modified.")

    def setValues(self,tblVals):
        global error3
        if self.Tbln == []:
            title = tblVals[0]
            obj = self.Table(title)
            obj.setTable(tblVals)
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
                obj.setTable(tblVals)
                self.Tbln.append(obj)#append to non-empty table list
                print("Table", title, "created.")

    def removeTable(self,tblName):
        i = 0
        for obj in self.Tbln:
            if self.Tbln[i].title == tblName:
                break
            else:
                i = i + 1#get index of table in use
        del self.Tbln[i]
        print("Table", tblName, "deleted.")
        
    def selectTable(self,tblName):
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
    class Table:
        def __init__(self, title):#default constructor
            self.title = title
            self.attr = []
            self.type = []
            self.len = []
        def setTable(self, vals):
            vals.reverse()
            self.title = vals.pop()
            while vals != []:
                self.attr.append(vals.pop())
                self.type.append(vals.pop())
                self.len.append(vals.pop())
        def modTable(self, vals):
            vals.reverse()
            self.title = vals.pop()
            self.attr.append(vals.pop())
            self.type.append(vals.pop())
            self.len.append(vals.pop())


def processExitKey():
    print("All Done.")

def processAlterKey(line):
    global dBn, inUse
    line = line.replace("ALTER TABLE ", '')
    line = line.split(";")[0]
    line = line.split(" ",-1)
    line.append(0)
    i = 0
    for obj in dBn:
        if dBn[i].name == inUse:
            break
        else:
            i = i + 1#get index of database in use
    dBn[i].modifyValues(line)



def processSelectKey(line):
    line = line.replace("SELECT * FROM ",'')
    global dBn, inUse,error5
    temp = line.split(';')[0]
    i = 0
    for obj in dBn:
        if dBn[i].name == inUse:
            break
        else:
            i = i + 1#get index of database in use
    if dBn[i].Tbln == []:
        error5 = 1#empty database
    for tableObj in dBn[i].Tbln:
        if tableObj.title == temp:
            dBn[i].selectTable(temp)
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

def processDropKey(line): 
    line = line.replace("DROP ",'')
    global dBn, error2, error4, inUse
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
                print("!Failed to delete", temp, "because it does not exist.")
        elif error4 == 1 or  dBn[i].Tbln == []:
            print("!Failed to delete", temp, "because it does not exist.")

def processCreateKey(line):
    line = line.replace("CREATE ",'')
    global dBn, error1, inUse
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

def loadDatabase(fname):
    dBfile = open(fname, "r")
    curLine = dBfile.readline()
    global inUse
    while curLine:
        curLine = dBfile.readline()
        if curLine.startswith("CREATE"):
            processCreateKey(curLine)
        elif curLine.startswith("DROP"):
            processDropKey(curLine)
        elif curLine.startswith("USE"):
            inUse = processUseKey(curLine)
        elif curLine.startswith("SELECT"):
            processSelectKey(curLine)
        elif curLine.startswith("ALTER"):
            processAlterKey(curLine)
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
