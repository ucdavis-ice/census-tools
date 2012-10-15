'''
Created on Oct 15, 2012

@author: roth
'''


import os, sys, csv, time, datetime, psycopg2

class Logger():
    def __init__(self):
        pass
    def OpenLog(self, path):
        self.log = open(path, "w")
    def WriteLog(self,msg):
        self.log.write(str(datetime.datetime.now()) + "|" + msg + "\n")
        print str(datetime.datetime.now()) + "|" + msg
    def CloseLog(self):
        self.log.close()

class MakeSequenceTables():
    def __init__(self):
        self.connstr = ""
        self.tblconfig = []
          
    def StartLog(self, path):
        self.logger = Logger()
        self.logger.OpenLog(path)
        self.logger.WriteLog("Begin Log")
        
    def CloseLog(self):
        self.logger.CloseLog()

    def OpenConnection(self, connstr):
        self.logger.WriteLog("Opening Connection")
        self.conn = psycopg2.connect(connstr)
        self.logger.WriteLog("Connection Open")
        self.connopen = True
        
    def CloseConnection(self):
        self.logger.WriteLog("Closing Connection")
        self.conn.close()
        self.connopen = False   
        self.logger.WriteLog("Connection Closed")
        
    def CleanCells(self,input):
        return input[:input.find(" CELLS")]
    
    def PadSeq(self,inseq):
        if len(inseq) == 1:
            return "00"+inseq
        elif len(inseq) == 2:
            return "0"+inseq
        elif len(inseq) == 3:
            return inseq
    def FixNum(self, instr):
        if len(instr) ==1:
            return "00"+instr
        elif len(instr) ==2:
            return "0"+instr
        else:
            return instr
    
    def MakeField(self, row):
        return [row[1].lower(), self.FixNum(row[3]), row[1].lower()+self.FixNum(row[3]), row[7]]
    
    def TestTabLen(self, tabconf, tablen):
        if len(tabconf) <> tablen:
            print "Table length error in table: " + tabconf[2]
    
    def MakeTableConfig(self, configfile, linepattern):
        self.logger.WriteLog("Building Table Config")
        cfile = open(configfile, 'rb')
        cfiler = csv.reader(cfile)
        seq = ""
        tab = ""
        #sord = 0
        #tord = 0
        allconf = [] # holds seqconfigs
        seqconf = [] # holds tabconfigs
        tabconf =[] # holds fields (in table(in seq)
        tablen = 0
        tabstart = 0
        i = 0
        for row in cfiler:
            #print row
            #if i > 110: break
            if i == 0:
                i+=1 #skip header row
            else:
                i+=1
                if seq == row[2]:
                    #Continue same sequence
                    if tab == row[1]:
                        
                        #Continue table
                        if row[3] <> "": 
                            if row[3][-2:] <>".5": tabconf.append(self.MakeField(row))
                    else:
                        #finish old table
                        self.TestTabLen(tabconf, tablen)
                        seqconf.append([tab.lower(),tablen, tabstart, tabconf])
                        #New table
                        tab =  row[1]
                        tabconf = []
                        if row[3] <> "":
                            if row[3][-2:] <>".5": tabconf.append(self.MakeField(row))
                        #tord=1
                        tabstart = int(row[4])
                        tablen = int(row[5][:row[5].find(" ")])
                        # add field data
                        
                else:
                    # finish old sequence
                    self.TestTabLen(tabconf, tablen)
                    if tabconf <> []:seqconf.append([tab.lower(),tablen, tabstart,tabconf])
                    if seqconf <> []:allconf.append([self.FixNum(seq),seqconf])
                    
                    #New sequence new table
                    seqconf = []
                    tabconf = []
                    if row[3] <> "":
                        if row[3][-2:] <>".5": tabconf.append(self.MakeField(row))
                    tab = row[1]
                    tabstart = int(row[4])
                    tablen = int(row[5][:row[5].find(" ")])
                    seq = row[2]
                    #sord = 1
        if tabconf <> []:seqconf.append([tab.lower(),tablen, tabstart,tabconf])
        if seqconf <> []:allconf.append([self.FixNum(seq),seqconf])            
        self.tblconfig = allconf
        self.logger.WriteLog("Built Table Config")
        
    def MakeTables(self, schema):
        self.logger.WriteLog("Making Tables")
        for seq in self.tblconfig:
            
            #unpack sequence
            seqnum =  seq[0]
            flds = ""
            for tbl in seq[1]:
                #unpack table
                tblname = tbl[0]
                
                for fld in tbl[3]:
                    # unpack fields
                    flds = flds + fld[2] + " NUMERIC,"
            sqlestimates = "CREATE TABLE " + schema + ".seq_e_"+seqnum +" (fileid varchar(6), filetype varchar(6), stusab varchar(2), chariter varchar(3), sequence varchar(4), logrecno varchar(7), " + flds[:-1] + ");"
            sqlmargins = "CREATE TABLE " + schema + ".seq_m_"+seqnum +" (fileid varchar(6), filetype varchar(6), stusab varchar(2), chariter varchar(3), sequence varchar(4), logrecno varchar(7), " + flds[:-1] + ");"
            #print sqlestimates
            #print sqlmargins
            c = self.conn.cursor()
            c.execute(sqlestimates)
            c.execute(sqlmargins)
            c = None
        self.conn.commit()
        self.logger.WriteLog("Tables Created")
        
    def DeleteTables(self, schema):
        self.logger.WriteLog("Dropping Tables")
        for seq in self.tblconfig:
            seqnum =  seq[0]
            sqlestimates = "DROP TABLE IF EXISTS " + schema + ".seq_e_"+seqnum +";"
            sqlmargins = "DROP TABLE IF EXISTS " + schema + ".seq_m_"+seqnum +";"
            c = self.conn.cursor()
            c.execute(sqlestimates)
            c.execute(sqlmargins)
            c = None
        self.conn.commit()
        self.logger.WriteLog("Tables Dropped")
    
    def FixVals(self, val):
        if val == '':
            return None
        elif val =='.':
            return None
        else:
            return val
    def ProcRow(self, row):
        orow = []
        for i in row:
            orow.append(self.FixVals(i))
        return orow
        
    def LoadSequences(self, schema, prefix, path):
        self.logger.WriteLog("Loading Tables")
        for seq in self.tblconfig:
            self.logger.WriteLog("Working on sequence:" + seq[0])
            #unpack sequences
            
            efile = path + "\\e"+ prefix + seq[0]+"000.txt"
            mfile = path + "\\m"+ prefix + seq[0]+"000.txt"
            
            #get number of fields
            nfields = 1
            for tbl in seq[1]:
                nfields = nfields + len(tbl[3])
                
            # Process estimates
            efilecsv = open(efile, "rb")
            csvefile = csv.reader(efilecsv)
            prepnfields = str("%s,"*nfields)[:-1]
            cur = self.conn.cursor()
            for row in csvefile:
                sqlins = "INSERT INTO "+ schema + ".seq_e_"+seq[0] + " VALUES ("+ prepnfields+");"
                preprow = self.ProcRow(row[5:])
                if len(preprow)<> nfields:
                    print "Number of fields does not match: " + seq[0]
                cur.execute(sqlins, preprow)
            self.conn.commit()
                
            mfilecsv = open(mfile, "rb")
            csvmfile = csv.reader(mfilecsv)
            prepnfields = str("%s,"*nfields)[:-1]
            
            for row in csvmfile:
                sqlins = "INSERT INTO "+ schema + ".seq_m_"+seq[0] + " VALUES ("+ prepnfields+");"
                preprow = self.ProcRow(row[5:])
                if len(preprow)<> nfields:
                    print "Number of fields does not match: " + seq[0]
                cur.execute(sqlins, preprow)
            self.conn.commit()
            cur = None
        self.logger.WriteLog("Tables Loaded")
    
    def LoadSequences2(self, schema, prefix, path):
        self.logger.WriteLog("Loading Tables")
        for seq in self.tblconfig:
            self.logger.WriteLog("Working on sequence:" + seq[0])
            #unpack sequences
            
            efile = path + "\\e"+ prefix + seq[0]+"000.txt"
            mfile = path + "\\m"+ prefix + seq[0]+"000.txt"
            
            #get number of fields
#            nfields = 1
#            for tbl in seq[1]:
#                nfields = nfields + len(tbl[3])
                
            # Process estimates
            efilecsv = open(efile, "rb")
#            csvefile = csv.reader(efilecsv)
#            prepnfields = str("%s,"*nfields)[:-1]
            cur = self.conn.cursor()
            cur.copy_from(efilecsv,schema + ".seq_e_"+seq[0], sep=",", null="")
            efilecsv = None
            mfilecsv = open(mfile, "rb")
            cur.copy_from(mfilecsv,schema + ".seq_m_"+seq[0], sep=",", null="")
            mfilecsv = None
#            for row in csvefile:
#                sqlins = "INSERT INTO "+ schema + ".seq_e_"+seq[0] + " VALUES ("+ prepnfields+");"
#                preprow = self.ProcRow(row[5:])
#                if len(preprow)<> nfields:
#                    print "Number of fields does not match: " + seq[0]
#                cur.execute(sqlins, preprow)
            self.conn.commit()
                
#            mfilecsv = open(mfile, "rb")
#            csvmfile = csv.reader(mfilecsv)
#            prepnfields = str("%s,"*nfields)[:-1]
#            
#            for row in csvmfile:
#                sqlins = "INSERT INTO "+ schema + ".seq_m_"+seq[0] + " VALUES ("+ prepnfields+");"
#                preprow = self.ProcRow(row[5:])
#                if len(preprow)<> nfields:
#                    print "Number of fields does not match: " + seq[0]
#                cur.execute(sqlins, preprow)
#            self.conn.commit()
#            cur = None
        self.logger.WriteLog("Tables Loaded")
    
    def MakeIndexes(self, schema):
        self.logger.WriteLog("Making indexes")
        for seq in self.tblconfig:
            self.logger.WriteLog("Indexing sequence:" + seq[0])
            sqlidxe ='CREATE INDEX seq_e_'+ seq[0] +'_idx ON acs_2010_5.seq_e_'+seq[0]+' USING btree (logrecno COLLATE pg_catalog."default" );'
            sqlidxm ='CREATE INDEX seq_m_'+ seq[0] +'_idx ON acs_2010_5.seq_m_'+seq[0]+' USING btree (logrecno COLLATE pg_catalog."default" );'
            cur = self.conn.cursor()
            cur.execute(sqlidxe)
            cur.execute(sqlidxm)
            self.conn.commit()
        cur = None
                         
class MakeGeoTable():
    def __init__(self):
        self.connopen =False
        self.fieldlist = []

    def StartLog(self, path):
        self.logger = Logger()
        self.logger.OpenLog(path)
        self.logger.WriteLog("Begin Log")
        
    def CloseLog(self):
        self.logger.CloseLog()

    def OpenConnection(self, connstr):
        self.logger.WriteLog("Opening Connection")
        self.conn = psycopg2.connect(connstr)
        self.logger.WriteLog("Connection Open")
        self.connopen = True
        
    def CloseConnection(self):
        self.logger.WriteLog("Closing Connection")
        self.conn.close()
        self.connopen = False   
        self.logger.WriteLog("Connection Closed")

    def MakeFieldList(self, configfile):
        self.logger.WriteLog("Making fieldlist from config file")
        cfile = open(configfile, "rb")
        reader = csv.reader(cfile)
        i = 0
        self.logger.WriteLog("looping through fields")
        for row in reader:
            if i > 0:
                fldname = str(row[0])
                fldstart = int(row[1])
                fldend = int(row[2])
                fldlen = fldend - fldstart + 1
                lst = [fldname, fldstart, fldend, fldlen]
                self.fieldlist.append(lst)
                i +=1
            else:
                i +=1

    def MakeTable(self,schema, tblname):
        self.logger.WriteLog("Making table from fieldlist")
        self.fulltablename =  schema + "." + tblname
        sqlstat = "CREATE TABLE " + self.fulltablename +" ("
        self.logger.WriteLog("looping through fields")
        i = 0
        for field in self.fieldlist:
                if i == 4:
                    sqlseg = field[0].lower() + " varchar("+ str(field[3])+") PRIMARY KEY,"
                else:
                    sqlseg = field[0].lower() + " varchar("+ str(field[3])+"),"
                sqlstat = sqlstat + sqlseg
                i += 1
        sqlstat = sqlstat[0:-1] + ") WITH OIDS;"
        self.logger.WriteLog("SQLStatement: "+ sqlstat)
        self.logger.WriteLog("Executing SQL")
        c = self.conn.cursor()
        c.execute(sqlstat)
        self.conn.commit()
        c = None
        self.logger.WriteLog("SQL Executed")
        
    def GetVals(self, row, cfg):
        return row[cfg[1]-1:cfg[1]+cfg[3]-1]
        
        
    
    def LoadTable(self, infile):
        #open source file
        ifile = open(infile, 'rb')
        c = self.conn.cursor()
        for row in ifile:
            inslist = []
            for field in self.fieldlist:
                inslist.append(self.GetVals(row,field))
            sqlstat = "INSERT INTO " + self.fulltablename + " VALUES ("+ str("%s,"*len(self.fieldlist))[:-1] + ");"#, inslist
            #print sqlstat
            try:
                c.execute(sqlstat, inslist)
            except Exception, e:
                print str(e)
                break
        self.conn.commit()
        c = None                  
                
   
        
if __name__ == "__main__":
    mst = MakeSequenceTables()
    mst.StartLog("log_sequences.txt")
    mst.OpenConnection("dbname='' user='' host='' password=''") # fix this as needed
    linepattern = ["file","tableid", "sequence","line", "startpos","cellsintable","cellsinsequence","tabletitle","subjarea"]
    configfile = "configfiles\\Sequence_Number_and_Table_Number_Lookup.txt"
    schema = "acs_test" # this schema needs to already exist.  
    
    mst.MakeTableConfig(configfile, linepattern)
    mst.DeleteTables(schema)
    mst.MakeTables(schema)
    mst.LoadSequences(schema, "20105ca0", "F:\\Census\\ACS_2006_2010\\BG_Tracts")
    mst.LoadSequences("acs_2010_5", "20105ca0", "F:\\Census\\ACS_2006_2010\\All_OtherGeom")
    mst.MakeIndexes(schema)
    
    
    mst.CloseConnection()
    mst.CloseLog()
    print "Done" 
    
    # Loading Geography Tables
    mgt = MakeGeoTable()
    mgt.StartLog("log_geographies.txt")
    mgt.OpenConnection("dbname='' user='' host='' password=''") # fix this as needed
    mgt.MakeFieldList("configfiles\\Geo_config.txt")
    mgt.MakeTable(schema, "acsgeo")
    mgt.LoadTable("configfiles\\g20105ca.txt")
    mgt.CloseConnection()
    mgt.CloseLog()
    print "Done"