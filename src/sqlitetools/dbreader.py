'''
Created on Jun 13, 2012

@package  ebf
@author   mpaegert
@version  \$Revision: 1.4 $
@date     \$Date: 2012/10/15 16:59:24 $

read and traverse sqlite database

$Log: dbreader.py,v $
Revision 1.4  2012/10/15 16:59:24  paegerm
Adding timeout of 10 s as default

Adding timeout of 10 s as default

Revision 1.3  2012/08/23 16:38:53  paegerm
allow to switch off the row factory

Revision 1.2  2012/08/16 22:21:53  paegerm
allow select parameter to be None for fetchmany and fetchone

Revision 1.1  2012/07/06 20:38:49  paegerm
Initial Revision

'''

import sqlite3


class DbReader(object):
    '''
    A reader for the sqlite databases
    '''
    
    def __init__(self, filename, factory = True, tout = 10.0):
        if (filename == None) or (len(filename) == 0):
            raise NameError(filename)
        
        self.fname = filename
        self.dbconn = sqlite3.connect(filename, timeout = tout)
        if (factory == True):
            self.dbconn.row_factory = sqlite3.Row
        self.dbcurs = self.dbconn.cursor()
        self.records = None
        
        
        
    def fetchall(self, select, args = None):
        if args == None:
            args = []
        self.dbcurs.execute(select, args)
        self.records = self.dbcurs.fetchall()
        return (self.records)
    
    
    def fetchmany(self, select = None, args = None, n = 1000):
        if args == None:
            args = []
        if (select != None):
            self.dbcurs.execute(select, args)
            self.dbcurs.arraysize = n
        self.records = self.dbcurs.fetchmany()
        return self.records
    
    
    def fetchone(self, select = None, args = None):
        if args == None:
            args = []
        if (select != None):
            self.dbcurs.execute(select, args)
        self.records = self.dbcurs.fetchone()
        return self.records
    
    
    
    def traverse(self, select, args = None, n = 1000):
        self.fetchmany(select, args, n)
        while (len(self.records) != 0):
            for star in self.records:
                yield star
            self.records = self.dbcurs.fetchmany()
            


    def getlc(self, staruid, tname = 'stars', order = None):
        if (staruid == None) or (staruid <= 0):
            raise ValueError
        cmd = 'select * from ' + tname + ' where staruid = ?'
        if (order != None):
            cmd += ' order by ' + order + ';'
        else:
            cmd += ';'
        self.records = self.fetchall(cmd, [staruid])
        return self.records
        
        
    def close(self):
        self.dbcurs.close()
        self.dbconn.close()
        

        
if __name__ == '__main__':
    pass
