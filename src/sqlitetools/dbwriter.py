'''
Created on Jun 18, 2012

@package  ebf
@author   mpaegert
@version  \$Revision: 1.2 $
@date     \$Date: 2012/08/16 22:21:53 $

read and traverse sqlite database

$Log: dbwriter.py,v $
Revision 1.2  2012/08/16 22:21:53  paegerm
*** empty log message ***

adding getlastuid, extending parameters for init (lf and noauto)

Revision 1.1  2012/07/06 20:38:49  paegerm
Initial Revision

'''

import sqlite3

from dbfunctions import *


class DbWriter(object):
    '''
    A reader for the sqlite databases
    '''
    def __init__(self, filename, cols, table = 'stars', types = None, 
                 nulls = None, lf = None, noauto = False):
        
        if (filename == None) or (len(filename) == 0):
            raise NameError(filename)
        
        if (types != None) and (nulls != None):
            create_db(filename, cols, types, nulls, table, lf, noauto)
        
        self.table = table
        self.fname = filename
        self.dbconn = sqlite3.connect(filename)
        self.dbconn.row_factory = sqlite3.Row
        self.dbcurs = self.dbconn.cursor()
        self.dbcurs.execute('PRAGMA synchronous=OFF;')
        self.inscmd = make_insert_statement(cols, table, '')
    
    
    def deletebystaruid(self, staruid):
        if (staruid == None) or (staruid <= 0):
            raise ValueError
        cmd = 'delete from ' + self.table + ' where staruid = ?;'
        self.dbcurs.execute(cmd, [staruid])
        self.dbconn.commit()


    def getlastuid(self):
        cmd = 'select max(uid) from ' + self.table + ';'
        res = self.dbcurs.execute(cmd)
        return res
        
        
    def insert(self, values, commit = False):
        if (self.inscmd == None):
            raise ValueError
        self.dbcurs.executemany(self.inscmd, values)
        if (commit == True):
            self.dbconn.commit()
        
        
    def update(self, cmd, values, commit = False):
        if (cmd == None):
            raise ValueError
        self.dbcurs.executemany(cmd, values)
        if (commit == True):
            self.dbconn.commit()


    def commit(self):
        self.dbconn.commit()
                
        
    def close(self):
        self.dbcurs.close()
        self.dbconn.close()
        