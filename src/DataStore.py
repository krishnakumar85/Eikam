# To implement data store object to hide data store policy

import sys
import csv
import sqlite3
import md5
import os

class Parser:
    pass

class CSVParser:
    def __init__(self, filename, hasHeader=True):
        self.filename = filename
        self.hasHeader = hasHeader
        ToolDataStore(self)
        # initialise ToolDatabase object for this file
    
    def get_header(self):
        self.fd = open(self.filename)
        dialect = csv.Sniffer().sniff(self.fd.readline())
        self.fd.seek(0); #reset position to start from beginning of file
        
        #detect header line
        #count non null fields(len1)
        #count non null fields of next line (len2). if len2 < len1, then first one is header
        headerIndex = 0
        index = 0
        for line in csv.reader(self.fd, dialect):
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
        self.fd.close()
        return retheader
    
    def parse(self):
        print CSVParser.__name__,"parse into DB"

class XLSParser:
    def __init__(self,filename):
        self.filename = filename

class DataStore(dict):
    pass

class InternalDataStore:
    """
        1) Stores information on headers, their aliases, type of data
    """
    def __init__(self, header):
        #check if header info is available in any of the types
        # 1) get tables
        # 2) get actual header names
        # 3) compare with existing headers
        # 4) determine alias names of headers to store in sqlite
        self.header = header
        
    def get_header_mapping(self):
        #TODO: db file from configuration
        db = sqlite3.connect("internal.db")
        cur = db.cursor()
        try:
            cur.execute("SELECT field,mapping from internal_header_mapping")
        except sqlite3.OperationalError, e:
            print "Error:",e
            cur.execute('CREATE TABLE internal_header_mapping(id INTEGER PRIMARY KEY, field CHAR, mapping CHAR)')
            db.commit()
        finally:
            mappings = cur.fetchall()
#            print mappings
        
        new_mappings = {}
        for thismapping in mappings:
            new_mappings[thismapping[0]] = thismapping[1]
#        print new_mapping
        ret_header_mapping = {}
        for header_field in self.header:
            if not new_mappings.has_key(header_field):
                # insert new mapping in table
                while True:
                    new_mapping = raw_input("Enter mapping for "+header_field+":")
                    #TODO: use regex to complete the sql field naming rules
                    if not new_mapping.__contains__(" "):
                        break
                cur.execute("INSERT INTO internal_header_mapping VALUES (NULL, ?, ?)", (header_field, new_mapping))
                db.commit()
            else:
                ret_header_mapping[header_field] = new_mappings[header_field]
        
        cur.close()
        db.close()
        return ret_header_mapping
        
        
class ToolDataStore:
    """
    1) Stores information about files, their md5sum, time stamps.
    Intended to be temporary.
    """
    def __init__(self, parser_obj):
        self.parser_obj = parser_obj
        self.filename = parser_obj.filename
#        print self.filename
        #TODO: time consuming, check based on configuration
        #FIXME: read() may not return till EOF
        self.md5 = 0 #this is a pseudo! use md5sum
        self.mtime = os.stat(self.filename).st_mtime
 #       print self.mtime
        
        # 1) get database entry for this file.
        # 2) Compare the entry with new entries.
        # 3) if equal, skip parsing
        # 4) if not, parse and commit to db
        db = sqlite3.connect("internal.db") #TODO: db file from configuration 
        cur = db.cursor()
        input_info = [None, None, None]
        try:
            cur.execute("SELECT filename, md5sum, mtime from tool_input_info WHERE filename=?",(self.filename,))
            input_info = cur.fetchall()
        except sqlite3.OperationalError, e:
            print "Error:",e
            
            #FIXME: try-except block for database stmnt execution
            cur.execute('CREATE TABLE tool_input_info(id INTEGER PRIMARY KEY, filename CHAR, md5sum CHAR, mtime INTEGER)')
            db.commit()
            cur.execute('INSERT INTO tool_input_info(id, filename, md5sum, mtime) VALUES(NULL, ?,?,?)',(self.filename, self.md5sum, self.mtime))
            db.commit()
            
        print input_info
#        print self.md5sum
        if not (input_info[0][0] == self.filename and input_info[0][2] == self.mtime):
            if not input_info[0][1] == self.md5sum:
                # parse into db
                print "call parse"
                self.parser_obj.parse()
        
        #FIXME: try-except block for database stmnt execution
        cur.execute("UPDATE tool_input_info SET md5sum=?, mtime=? WHERE filename=?",(self.md5sum, self.mtime, self.filename))
        db.commit()
        
    def __getattr__(self, name):
 #       print "here in getattr"
        if name == "md5sum":
            if self.md5 == 0: #not computed before
#                print "compute md5sum"
                self.md5 = md5.new(open(self.filename).read()).hexdigest()
            return self.md5
            

if __name__ == "__main__":
    crdcparsed = CSVParser("..\crdc.csv")
    print crdcparsed.get_header()
    intstore = InternalDataStore(crdcparsed.get_header())
    print intstore.get_header_mapping()
