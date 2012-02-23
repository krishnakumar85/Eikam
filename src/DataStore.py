# To implement data store object to hide data store policy
# TODO: Refactor code into multiple files

import sys
import sqlite3
import os
import string

class DataStore(dict):
    pass

class InternalDataStore:
    """
        1) Stores information on headers, their aliases, type of data
    """
    def __init__(self):
        #check if header info is available in any of the types
        # 1) get tables
        # 2) get actual header names
        # 3) compare with existing headers
        # 4) determine alias names of headers to store in sqlite
        self.header = None
        self.header_mapping = None
        self.mapped_header = None
        
    def get_header_mapping(self, header):
        self.header = header
        
        #TODO: db file from configuration
        db = sqlite3.connect("internal.db")
        cur = db.cursor()
        try:
            cur.execute("SELECT field,mapping from internal_header_mapping")
            mappings = cur.fetchall()
        except sqlite3.OperationalError, e:
            print "Error:",e
            cur.execute('CREATE TABLE internal_header_mapping(id INTEGER PRIMARY KEY, field CHAR, mapping CHAR)')
            db.commit()
            cur.execute("SELECT field,mapping from internal_header_mapping")
            mappings = cur.fetchall()
#            print mappings
        
        new_mappings = {}
        for thismapping in mappings:
            new_mappings[thismapping[0]] = thismapping[1]
#        print new_mapping
        ret_header_mapping = {}
        self.mapped_header = [] #required to maintain order of the mapped header list
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
                
                ret_header_mapping[header_field] = new_mapping
                self.mapped_header.append(new_mapping)
            else:
                ret_header_mapping[header_field] = new_mappings[header_field]
                self.mapped_header.append(new_mappings[header_field])
        #print "MAPPED HEADER",self.mapped_header
        cur.close()
        db.close()
        
        self.header_mapping = ret_header_mapping
        return ret_header_mapping
        
        
class ToolDataStore:
    """
    1) Stores information about files, their md5sum, time stamps.
    Intended to be temporary.
    """
    def __init__(self, parser_obj):
        self.parser_obj = parser_obj
        self.filename = None
#        print self.filename
        #TODO: time consuming, check based on configuration
        #FIXME: read() may not return till EOF
        self.md5sum = 0
        self.mtime = None
 #       print self.mtime
        #clean_filename is the table name for storing parsed records
        replace_space_uscore = string.maketrans(' ', '_')
        self.clean_filename = os.path.basename(self.parser_obj.filename).rsplit('.')[0].lower().translate(replace_space_uscore)
        
        # 1) get database entry for this file.
        # 2) Compare the entry with new entries.
        # 3) if equal, skip parsing
        # 4) if not, parse and commit to self.db
        self.db = sqlite3.connect("internal.db") #TODO: self.db file from configuration 
        cur = self.db.cursor()
        input_info = [None, None, None]
        try:
            cur.execute("SELECT filename, md5sum, mtime from tool_input_info WHERE filename=?",(self.parser_obj.filename,))
            input_info = cur.fetchall()
        except sqlite3.OperationalError, e:
            print "Error:",e
            
            #FIXME: try-except block for database stmnt execution
            cur.execute('CREATE TABLE tool_input_info(id INTEGER PRIMARY KEY, filename CHAR, md5sum CHAR, mtime INTEGER)')
            self.db.commit()
            cur.execute('INSERT INTO tool_input_info(id, filename, md5sum, mtime) VALUES(NULL, ?,?,?)',(self.parser_obj.filename, self.md5sum, self.mtime))
            self.db.commit()
            cur.execute("SELECT filename, md5sum, mtime from tool_input_info WHERE filename=?",(self.parser_obj.filename,))
            input_info = cur.fetchall()
            
        #print input_info
        self.filename = input_info[0][0]
        self.mtime = input_info[0][2]
        self.md5sum = input_info[0][1]
     
    def reinit(self):
        #drop table, create table
        #print "REINIT"
        cur = self.db.cursor()
        cur.execute("DROP TABLE IF EXISTS "+self.clean_filename)
        self.db.commit()
        
        mapped_header = self.parser_obj.internalstore.mapped_header
        print type(mapped_header)
        table_fields = string.join(mapped_header,",")
        
        #print "table_fieds",table_fields
        cur.execute("CREATE TABLE "+self.clean_filename+"("+table_fields+")")
        self.db.commit()
        
    def insert_parsed_record(self,record_data):
        cur = self.db.cursor()

        mapped_header = self.parser_obj.internalstore.mapped_header
        #print mapped_header
        table_fields = string.join(mapped_header,",")
        #print record_data
        
        q_val = string.join(("? "*len(record_data)).split(),",")
        cur.execute("INSERT INTO "+self.clean_filename+"("+table_fields+") "+" VALUES ("+q_val+")", (record_data))
        self.db.commit()
        
        #FIXME: update must be present only after the parsed content is stored in the db.
        cur.execute("UPDATE tool_input_info SET md5sum=?, mtime=? WHERE filename=?",(self.parser_obj.md5sum, self.parser_obj.mtime, self.parser_obj.filename))
        self.db.commit()
        #print "Data comitted"

    
    
    
