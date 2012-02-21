import os
import csv
import md5
from DataStore import *

class Parser:
    pass

class CSVParser:
    def __init__(self, filename, hasHeader=True):
        self.filename = filename
        self.hasHeader = hasHeader
        self.header = None
        self.mtime = os.stat(self.filename).st_mtime
        self.__md5 = 0 #use md5sum instead of md5
        self.mystore = ToolDataStore(self)
        self.internalstore = InternalDataStore()
        # initialise ToolDatabase object for this file
    
    def get_header(self):
        
        if self.header != None:
            return self.header
        
        self.fd = open(self.filename)
        self.dialect = csv.Sniffer().sniff(self.fd.readline())
        self.fd.seek(0); #reset position to start from beginning of file
        
        #detect header line
        #count non null fields(len1)
        #count non null fields of next line (len2). if len2 < len1, then first one is header
        headerIndex = 0
        index = 0
        for line in csv.reader(self.fd, self.dialect):
            nfields = len(line)
            countfields = 0
            index += 1
            for field in line:
                if(len(field) != 0):
                    countfields += 1
            if nfields == countfields:
                headerIndex = index
                retheader = line
                break
        #self.fd.close() # For continuing the flow while parsing, this fd must remain uncommented.
        
        self.header = retheader
        return retheader
    
    def parse(self):
        self.get_header() #automatically sets self.header
        self.internalstore.get_header_mapping(self.header)
        
        if (self.mystore.filename == self.filename and self.mystore.mtime == self.mtime):
            if self.mystore.md5sum == self.md5sum:
                # parse into self.db
                return
        
        self.get_header()
        self.mystore.reinit()
        for line in csv.reader(self.fd, self.dialect):
            print line
            self.mystore.insert_parsed_record(line)
        self.fd.close()
        
    def __getattr__(self, name):
 #       print "here in getattr"
        if name == "md5sum":
            if self.__md5 == 0: #not computed before
#                print "compute md5sum"
                self.__md5 = md5.new(open(self.filename).read()).hexdigest()
            return self.__md5

class XLSParser:
    def __init__(self,filename):
        self.filename = filename