#!/usr/bin/python2
# -*- coding: utf-8 -*-
#2018.10.02
#(c) Mamykin Sergey
#s-a-m@yandex.ru
import fnmatch
import fdb
import os
import sys
import threading
import time
import datetime
import uuid, math
import traceback
from urlparse import urlparse
import urllib
###################################################################
from squid_log_importer_cfg import PROXY_ID
from squid_log_importer_cfg import LOG_DIR
from squid_log_importer_cfg import LOG_FILE_MASKS
from squid_log_importer_cfg import DB_CON
from squid_log_importer_cfg import DB_USER
from squid_log_importer_cfg import DB_PASSWORD
from squid_log_importer_cfg import LOG_FILE_NAME
from squid_log_importer_cfg import RECORDS_TO_COMMIT
from squid_log_importer_cfg import MIN_BYTES_TO_REGISTER
###################################################################
MinBytesToRecordCnt = 0
MinBytesToRecordInFileCnt = 0
MinBytesSum = 0
MinBytesInFileSum = 0
LinesInsertedTotal = 0
RegistredBytes = 0
RegistredBytesInFile = 0
LinesTotal = 0
Recs = 0
BadLinesCnt = 0
BadLinesInFileCnt = 0
FilesCnt = 0
BadLinesCnt = 0
LinesTotal = 0
FullCurFileName = ''
LineNumber = 0

Con = fdb.connect(dsn = DB_CON, user = DB_USER, password = DB_PASSWORD, charset='UTF8')
Cur = Con.cursor()
Cur.execute("select SQL_TEXT from SYS$SQLS where ID = 1")
SQL_INSERT_LINE =  '%s' % Cur.fetchone();
#print SQL_INSERT_LINE
#for SQL_TEXT in Cur:
#    SQL_INSERT_LINE = '%s' % SQL_TEXT

Cur.execute("select SQL_TEXT from SYS$SQLS where ID = 2")
SQL_LOG_TO_DB =  '%s' % Cur.fetchone();
#for SQL_TEXT in Cur:
#    SQL_LOG_TO_DB = '%s' % SQL_TEXT    
#print SQL_LOG_TO_DB
SessionID = uuid.uuid1().hex

def main():
    global FilesCnt
    dt = datetime.datetime.now()

    #############testing
    #SQL = "delete from TRAFF_BUFF_P"
    #Con.execute_immediate(SQL)
    #Con.commit()
    #############testing


    if (LOG_FILE_NAME != ''):
        mylogfile = open(LOG_FILE_NAME, 'a')
        mylogfile.write('################################################\n')
    for mask in LOG_FILE_MASKS:
        for file in os.listdir(LOG_DIR):
            if fnmatch.fnmatch(file, mask):
                ImportFile(file, mylogfile)
                FilesCnt += 1
    Con.commit()
    et = get_elapsed_time(dt)
    if (LOG_FILE_NAME != ''):
        add_log_line('Total:', mylogfile)
        add_log_line(' Session ID: %s' % SessionID, mylogfile)
        add_log_line(' Files: %s' % FilesCnt, mylogfile)
        add_log_line(' Lines: %s' % LinesTotal, mylogfile)
        add_log_line(' Bad lines: %s' % BadLinesCnt, mylogfile)
        add_log_line(' Tiny bytes (< %s): %s lines, %s bytes' % (MIN_BYTES_TO_REGISTER, MinBytesToRecordCnt, MinBytesSum), mylogfile)    
        add_log_line(' Lines taken: %s' % LinesInsertedTotal, mylogfile)
        add_log_line(' Registred bytes: %s' % RegistredBytes, mylogfile)
        add_log_line(' Elapsed time: %s s' % et, mylogfile)
        add_log_line(' DB: %s user: %s' % (DB_CON, DB_USER), mylogfile)
        add_log_line(' Proxy_ID: %s' % PROXY_ID, mylogfile)
        mylogfile.write('################################################\n')
        mylogfile.close()
    print 'Total:'
    print ' Session ID: %s' % SessionID
    print ' Files: %s' % FilesCnt
    print ' Lines: %s' % LinesTotal
    print ' Bad lines: %s' % BadLinesCnt
    print ' Tiny bytes (< %s): %s lines, %s bytes' % (MIN_BYTES_TO_REGISTER, MinBytesToRecordCnt, MinBytesSum)
    print ' Lines taken: %s' % LinesInsertedTotal
    print ' Registred bytes: %s' % RegistredBytes
    print ' Elapsed time: %s' % et

def get_elapsed_time(start):
	delta = datetime.datetime.now() - start
	days = int(delta.days)
	hours = int(delta.seconds / 3600)
	minutes = int((delta.seconds - hours * 3600) / 60)
	seconds = delta.seconds - hours * 3600 - minutes * 60 \
		+ float(delta.microseconds) / 1000 / 1000
	result = ''
	if days:
		result += '%dd ' % days
	if days or hours:
		result += '%dh ' % hours
	if days or hours or minutes:
		result += '%dm ' % minutes
	return '%s%.3fs' % (result, seconds)

def get_file_range(Afile, ALogFIle):
    global UT_start, UT_end, FromLine, ToLine, BadLinesCnt, BadLinesInFileCnt
    FromLine = 0
    ToLine = 0
    UT_start = 0
    UT_end = 0
    first_space_idx = 0
    LineNumber = 0
    DC = 0
    for l in Afile:
        l = l.strip()
        if (LineNumber >= 0) and (l <> ""):
            first_space_idx = l.find(" ", 0, 15)
            if first_space_idx > 0:
               if DC == 0:
                  UT_start = l[0:first_space_idx]
                  FromLine = LineNumber     
                  DC = 1
               UT_end = l[0:first_space_idx]
               ToLine = LineNumber 
        LineNumber += 1
    FromLine += 1
    ToLine += 1

    
    try:
        Unixtime = UT_start.split('.')
        Secs = int(Unixtime[0])
        Subsecs = int(Unixtime[1])
    except:
            BadLinesCnt += 1
            BadLinesInFileCnt += 1 
            add_log_line('Bad start time {0:d}'.format(UT_start),  ALogFIle)
            return 0
        
    
    try:
        Unixtime = UT_end.split('.')
        Secs = int(Unixtime[0])
        Subsecs = int(Unixtime[1])
    except:
            BadLinesCnt += 1
            BadLinesInFileCnt += 1
            add_log_line('Bad end time {0:d}'.format(UT_end), ALogFIle)
            return 0        

    
    #print UT_start
    #print UT_end
    #print FromLine
    #print ToLine

    return 1

def add_log_line(Aline, AFile):
    if (LOG_FILE_NAME != ''):
        L = Aline
        now = datetime.datetime.now().isoformat()
        AFile.write(now + ' | ' + L + '\n')
    return 1

def method_is_wrong(AMethod):
    HTTP_Methods = ['CONNECT', 'GET', 'POST', 'NONE', 'PUT', 'OPTIONS', 'HEAD',
                    'DELETE', 'TRACE', 'PATCH', 'PROPFIND', 'LINK', 'UNLINK', 'PROPATCH',
                    'ICP_QUERY', 'PURGE', 'MKCOL', 'MOVE', 'COPY', 'LOCK', 'UNLOCK']
    if (AMethod not in HTTP_Methods):
        R = 1
    else:
        R = 0
    return R
#@profile

def insert_line(Aline, AOpCode, ALogFIle):
    global MinBytesToRecordCnt, MinBytesSum, LinesInsertedTotal, RegistredBytes
    global Recs, BadLinesCnt
    global BadLinesInFileCnt, MinBytesToRecordInFileCnt, MinBytesInFileSum, RegistredBytesInFile 
    #re.sub(r'\s+', ' ', Aline) # delete spaces
    Flds = Aline.split()
    #Flds = [x for x in Flds if x != ''] #delete empty values in list
    Cn = len(Flds)
    #mylogfile.write('----------------------------------------\n')
    #mylogfile.write(Aline)
    if (Cn > 13):
        BadLinesCnt += 1
        BadLinesInFileCnt += 1
        add_log_line('Bad line from log ('+ str(Cn) + ' fields): ' + Aline.rstrip('\n'), ALogFIle)
        return 0
    try:
         Bytes = int(Flds[4])
    except:
            BadLinesCnt += 1
            BadLinesInFileCnt += 1
            add_log_line('Bad size of reply(' + Flds[4] +'): ' + Aline.rstrip('\n'), ALogFIle)
            return 0
    if (Bytes < MIN_BYTES_TO_REGISTER):
        MinBytesToRecordCnt += 1
        MinBytesToRecordInFileCnt += 1
        MinBytesSum = MinBytesSum + Bytes
        MinBytesInFileSum = MinBytesInFileSum + Bytes
        return 0
    
    try:
        Unixtime = Flds[0].split('.')
        Secs = int(Unixtime[0])
        Subsecs = int(Unixtime[1])
    except:
            BadLinesCnt += 1
            BadLinesInFileCnt += 1
            add_log_line('Bad unix time (' + Flds[0] +'): ' + Aline.rstrip('\n'), ALogFIle)
            return 0
    try:
        TimeResponse = int(Flds[1])
    except:
            BadLinesCnt += 1
            BadLinesInFileCnt += 1
            add_log_line('Bad time response (' + Flds[1] +'): ' + Aline.rstrip('\n'), ALogFIle)
            return 0        
    ClientIP = Flds[2]
    RSSH = Flds[3].split('/')
    ReqStatus = RSSH[0] 
    try:
         HTTPCode = int(RSSH[1])
    except:
            BadLinesCnt += 1
            BadLinesInFileCnt += 1
            add_log_line('Bad request status\HTTP code(' + Flds[3] +'): ' + Aline.rstrip('\n'), ALogFIle)
            return 0
    ReqMeth = Flds[5]
    if (method_is_wrong(ReqMeth) == 1):
        BadLinesCnt += 1
        BadLinesInFileCnt += 1
        add_log_line('Bad HTTP method (' + ReqMeth + '): ' + Aline.rstrip('\n'), ALogFIle)
        return 0
    ReqURL = Flds[6]
    UName = Flds[7]
    Hier = Flds[8]
    MimeType = Flds[9]
    SNI = Flds[10]
    UserAgent = ''
    #UserAgent = Flds[11]
    #urllib.unquote(UserAgent).decode('utf8') 
    URL = urlparse(ReqURL)
    
    Host  = URL.hostname
    Port = URL.port
    UHost =  ''
    UPort = ''
    if (Host == None):
        UHost = ''
    else:
        UHost = Host
    if (Port == None):
        UPort = 80
    else:
        UPort = Port
    UPath = URL.path
    #urllib.unquote(UPath).decode('utf8') 
    UScheme = URL.scheme
    if ((UPath <> '') and (UScheme == '') and (Port == None) and (Host == None)):
        Https = UPath.split(':')
        UHost = Https[0]
        SPort = 443
        if (len(Https) == 2):
            try:
                 SPort = int(Https[1])
            except:
                    BadLinesCnt += 1
                    BadLinesInFileCnt += 1
                    add_log_line('Bad port(' + Https[1] +'): ' + Aline.rstrip('\n'), ALogFIle)
                    return 0
        UPort = SPort
        UPath = ''
        UScheme = 'https'
    if (len(UHost) > 255):
        UHost = UHost[:255]
    if (len(ReqStatus) > 40):
        ReqStatus = ReqStatus[:40]
    if (len(ReqMeth) > 40):
        ReqMeth = ReqMeth[:40]    
    if (len(UName) > 40):
        UName = UName[:40]  
    if (len(Hier) > 120):
        Hier = Hier[:120]
    if (len(MimeType) > 120):
        MimeType = MimeType[:120]
    if (len(UserAgent) > 250):
        UserAgent = UserAgent[:250]         
    if (len(UPath) > 4096):
        UPath = UPath[:4096]        
    UPath = UPath.replace("'", "''")
    MimeType = MimeType.replace("'", "''")
    #print SNI
    try:
        SQL = ''
        SQL = SQL_INSERT_LINE % (AOpCode, Secs, Subsecs, TimeResponse,
                                 ClientIP, ReqStatus, HTTPCode, Bytes,
                                 ReqMeth, UScheme, UHost, UPort,
                                 UPath, UName, Hier, MimeType.rstrip('\n'), SNI, UserAgent)
    except UnicodeDecodeError:
                              #for F in Flds:
                              #    print F
                                  
                              
                              print 'UnicodeDecodeError(line %s): "%s", "%s", "%s", "%s", "%s"' % (LineNumber, UPath, UHost, MimeType, SNI, UserAgent)
                              UPath = "".join([x if ord(x) < 128 else '?' for x in UPath])
                              UHost = "".join([x if ord(x) < 128 else '?' for x in UHost])
                              MimeType = "".join([x if ord(x) < 128 else '?' for x in MimeType])
                              SNI = "".join([x if ord(x) < 128 else '?' for x in SNI])
                              print 'Converted to "%s", "%s", "%s", "%s"' % (UPath, UHost, MimeType, SNI)
                              add_log_line('UnicodeDecodeError: ' + Aline.rstrip('\n'), ALogFIle)
                              #return 0
                              #print UserAgent
                              
                              SQL = SQL_INSERT_LINE % (AOpCode, Secs, Subsecs, TimeResponse,
                                                       ClientIP, ReqStatus, HTTPCode, Bytes,
                                                       ReqMeth, UScheme, UHost, UPort,
                                                       UPath, UName, Hier, MimeType.rstrip('\n'), SNI, UserAgent)
                                                       
                                                       
                              #print SQL 
                              add_log_line('UnicodeDecodeError: ' + Aline.rstrip('\n'), ALogFIle)
                              add_log_line('Converted to "%s", "%s", "%s", "%s"' % (UPath, UHost, MimeType, SNI), ALogFIle)
                              add_log_line(SQL, ALogFIle)
                              
    try:     
        Con.execute_immediate(SQL)
      #  add_log_line(SQL, ALogFIle)
       # add_log_line('Con.execute_immediate(SQL)', ALogFIle)
        LinesInsertedTotal += 1
        RegistredBytes = RegistredBytes + Bytes
        RegistredBytesInFile = RegistredBytesInFile + Bytes
        Recs += 1
        if (Recs >= RECORDS_TO_COMMIT):
            SQL = SQL_LOG_TO_DB % (AOpCode, SessionID, PROXY_ID, DT_START, -1, FullCurFileName,
                                   FromLine, UT_start, ToLine, UT_end, LineNumber, MinBytesToRecordInFileCnt, MinBytesInFileSum,
                                   LinesInserted + 1, RegistredBytesInFile, BadLinesInFileCnt, -1)

            #add_log_line(SQL, ALogFIle)
            Con.execute_immediate(SQL)
            Con.commit()
            add_log_line(SQL, ALogFIle)
            add_log_line('Con.commit()', ALogFIle)
            #add_log_line(SQL, ALogFIle)
            print 'Records committed total: %s' % (LinesInsertedTotal)
            #if LinesInsertedTotal > 20000:
                #s = sqrt(-1)
            Recs = 0
        #xx= -1
        #a = math.sqrt(xx)
    except:
            ErrText = 'Error at line %s: %sQuery: %s' % (LineNumber, traceback.format_exc().replace("'", "''"), SQL.replace("'", "''"))
            add_log_line(ErrText, ALogFIle)
            SL = "update PROXY_IMPORT_LOG set LAST_ERROR = '%s' where OP_CODE = '%s'" % (ErrText, AOpCode)
            Con.rollback()
            Con.execute_immediate(SL)
            Con.commit()
            print ErrText
            raise
    return 1

def ImportFile(Afilename, ALogFIle):
    global LinesTotal, FullCurFileName, LineNumber, DT_START, LinesInserted
    global BadLinesInFileCnt, MinBytesToRecordInFileCnt, MinBytesInFileSum, RegistredBytesInFile
    BadLinesInFileCnt = 0
    MinBytesToRecordInFileCnt = 0
    MinBytesInFileSum = 0
    RegistredBytesInFile = 0
    FullCurFileName = LOG_DIR + Afilename
    print ('Processing logfile: ' + FullCurFileName)
    OpCode = uuid.uuid1().hex
    dt = datetime.datetime.now()
    Start_time = dt.strftime("%Y-%m-%d %H:%M:%S")
    DT_START = time.mktime(dt.timetuple()) + float(dt.microsecond)/1000000
    add_log_line('Op. code: ' + OpCode, ALogFIle)
    add_log_line('Logfile: ' + FullCurFileName, ALogFIle)
    #MD5 = get_file_md5(FullCurFileName)
    #add_log_line('MD5: ' + MD5)
    #print SQL_LOG_TO_DB

    inputfile = open(FullCurFileName)
    if get_file_range(inputfile, ALogFIle) <> 1:
        return 0
    add_log_line('Start line: %s' % FromLine, ALogFIle)
    add_log_line('UT start: %s' % UT_start, ALogFIle)
    add_log_line('End line: %s' % ToLine, ALogFIle)
    add_log_line('UT end: %s' % UT_end, ALogFIle)


    SQL = '' 
    SQL = SQL_LOG_TO_DB % (OpCode, SessionID, PROXY_ID, DT_START, -1, FullCurFileName, FromLine, UT_start, -1, -1, -1, -1, -1, -1, -1, -1, -1)
    add_log_line(SQL, ALogFIle)
    Con.execute_immediate(SQL)
    Con.commit()
         
   
    LineNumber = 0
    LinesInserted = 0
    inputfile = open(FullCurFileName)
    for line in inputfile:
        LineNumber += 1
        LinesTotal += 1
        if insert_line(line, OpCode, ALogFIle) == 1:
            LinesInserted += 1
            if LineNumber % RECORDS_TO_COMMIT == 0:
                print 'Line %s of %s' % (LineNumber, ToLine) 
    Con.commit()
    print 'Line %s of %s' % (LineNumber, ToLine) 
    print 'Lines commited total: %s' % (LinesInsertedTotal)
    add_log_line('Lines in file: %s' % LineNumber, ALogFIle)
    add_log_line('Bad lines in file: %s' % BadLinesInFileCnt, ALogFIle)
    add_log_line('Tiny bytes (< %s): %s lines, %s bytes' % (MIN_BYTES_TO_REGISTER, MinBytesToRecordInFileCnt, MinBytesInFileSum), ALogFIle)    
    add_log_line('Lines taken: %s' % LinesInserted, ALogFIle)
    add_log_line('Registred bytes: %s' % RegistredBytesInFile, ALogFIle)
    inputfile.close()
    TDelta = get_elapsed_time(dt)
    dt = datetime.datetime.now()
    End_time = dt.strftime("%Y-%m-%d %H:%M:%S")
    DT_END = time.mktime(dt.timetuple()) + float(dt.microsecond)/1000000
    add_log_line('Start time: %s' % Start_time, ALogFIle)
    add_log_line('End time: %s' % End_time, ALogFIle)
    add_log_line('Elapsed time: %s' % TDelta, ALogFIle)
#### comment this:
    #add_log_line('DT_START: %s' % DT_START, ALogFIle)
    #add_log_line('DT_END: %s' % DT_END, ALogFIle)
####    
    add_log_line('Last line: %s' % line, ALogFIle)

    SQL = SQL_LOG_TO_DB % (OpCode, SessionID, PROXY_ID, DT_START, DT_END, FullCurFileName,
                           FromLine, UT_start, ToLine, UT_end, LineNumber, MinBytesToRecordInFileCnt, MinBytesInFileSum,
                           LinesInserted, RegistredBytesInFile, BadLinesInFileCnt, 0)

    #add_log_line(SQL, ALogFIle)
    Con.execute_immediate(SQL)
    Con.commit()
    
    return 1

main()
