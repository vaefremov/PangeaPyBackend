#!/opt/PANGmisc/bin/python
# $Id: logtools.py 5880 2008-03-20 11:47:35Z efremov $
#
# This modules incapsulates communication utilities

__version__ = "$Revision: 5880 $"[11:-2]  # code version

import time
import os
import syslog
from . import koi2volapyuk
import codecs

class SimpleLogTool:
    '''Used to log interactions via Helper class'''
    def __init__(self, on_initially = 1, max_len = 10, ident = None):
        """Input:
        on_initially
        max_len
        ident         = identifier of logged subsystem. If None - logging
                        is made through stderr
        """
        self.log = []
        self.on = on_initially # verbosity level
        self.max_len = max_len
        self.pid = os.getpid()
        self.charset = 'utf8' # charset used to output messages to stderr and syslog
        self.ident = ident
        if ident:
            self.ident = str(ident)
            syslog.openlog(self.ident, syslog.LOG_PID)
        
    def addLine(self, line, priority = syslog.LOG_INFO):
        """Add line to the log. Lines are added always.
        """
        line = str(line)
        if self.ident:
            syslog.syslog(priority, codecs.encode(line, self.charset))
        else:
            print >>sys.stderr, '%s[%d]:' % (time.asctime(), self.pid), codecs.encode(line, self.charset)
        cur_len = len(self.log)
        if cur_len >= self.max_len:
            self.log = self.log[(self.max_len / 2) :]
        self.log.append(line)

    def addLineWithID(self, id, function, line = None):
        '''Add line to log with function name and Id of caller'''
        if self.on:
            l = str(id) + '/' + str(function)
            if line:
                l += ': ' + str(line)
            self.addLine(l)

    def error(self, message, ex = None):
        """Reports errors. Message is always output"""
        l = "ERROR: " + str(message)
        if ex:
            l += " " + str(ex)
        self.addLine(l, syslog.LOG_ERR)

    def warning(self, message, ex = None):
        """Reports errors. Message is always output"""
        l = "WARNING: " + str(message)
        if ex:
            l += " " + str(ex)
        self.addLine(l, syslog.LOG_WARNING)

    def debug(self, message, ex = None):
        """Output debug message"""
        if self.on > 1:
            l = "DEBUG: " + str(message)
            if ex:
                l += " " + str(ex)
            self.addLine(l, syslog.LOG_DEBUG)
        
    def clear(self):
        self.log = []

    def logOn(self, verbosity = 1):
        """Switch log on"""
        if verbosity < 1:
            verbosity = 1
        self.on = verbosity

    def logOff(self):
        """Switch log off"""
        self.clear()
        self.on = 0

    def isOn(self):
        return self.on

    def getLog(self):
        return self.log

############################
## test part
###########################
if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        log = SimpleLogTool(2, 10, sys.argv[1])
    else:
        log = SimpleLogTool(2, 10)
    log.addLineWithID(1, 'Message from id 1')
    log.addLine('Message 2')
    for i in range(10):
        log.addLineWithID(i, 'Message from id %d' %i)
    log.error('That was error', 'Exception...')
    log.debug('Debug message', 'Additional message')
    log.warning('That was warning', 'Exception...')
    
    # switch off
    print('=========== log off')
    log.logOff()
    for i in range(10):
        log.addLineWithID(i, 'Invisible message from id %d' %i)
    log.error('That was error', 'Exception...')
    log.debug('Debug message', 'Additional message')

    print ('=========== level 1')
    log.logOn(1)  # verbosity 1
    log.error('That was error', 'Exception...')
    log.debug('Debug message', 'Additional message')
    log.addLineWithID(1, 'Message from id 1 on level 1')

    # Cyrillic letters:
    print ('=========== Cyrillic letters')
    log.logOn(3)  # full verbosity 
    log.debug('Debug message: '+ koi2volapyuk.ruscaps, 'Additional message: ' + koi2volapyuk.ruslett)

    # message log
    print ('=========== message log')
    print (log.getLog())
    
