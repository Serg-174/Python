#!/usr/bin/python2
# -*- coding: cp1251 -*-
#2017.02.02
######################################################################
#Proxy id (Integer)
PROXY_ID = 1
#log directory (final slash is mandatory)
#LOG_DIR = '/mydata/logs/squid/'
#LOG_DIR = 'D:/logs/'
LOG_DIR = 'D:/Work/_py/squid/logs/'
#Import from this files
#LOG_FILE_MASKS = ['access.log', 'access.log.2*', 'access.log.4*']
LOG_FILE_MASKS = ['access2.log*']
#LOG_FILE_MASKS = ['log_daemon2.log*']
#LOG_FILE_MASKS = ['aaa.log']
#Database connection string
#DB_CON = 'D:/DB/SQUIDLOG.FDB'
DB_CON = 'VOLODYA:TRAFF'
#DB_CON = '105.65.1.1:SQUIDLOG'
# Firebird user
DB_USER = 'SYSDBA'
# Firebird password
DB_PASSWORD = '***************'
#Logging operation file (if empty string, than do not log)
LOG_FILE_NAME ='D:/Work/_py/squid/squid_log_importer.log'
#LOG_FILE_NAME ='D:/Logs/squid_log_importer.log'
#LOG_FILE_NAME ='/mydata/logs/squid/squid_log_importer.log'
#LOG_FILE_NAME =''
######################################################################
RECORDS_TO_COMMIT = 10000
MIN_BYTES_TO_REGISTER = 300
#++++++++++++++++++++++++++


