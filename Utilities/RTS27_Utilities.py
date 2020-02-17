#!/usr/bin/python

import pymysql, sys
import logging
from decimal import Decimal

class RTS27_TableSwitches:

    PROCESS_TABLE_1 = "N"
    PROCESS_TABLE_2 = "N"
    PROCESS_TABLE_3 = "N"
    PROCESS_TABLE_4 = "N"
    PROCESS_TABLE_6 = "N"

    def __init__(self, PROCESS_TABLE_1, PROCESS_TABLE_2,PROCESS_TABLE_3, PROCESS_TABLE_4 , PROCESS_TABLE_6):

        self.PROCESS_TABLE_1 = PROCESS_TABLE_1
        self.PROCESS_TABLE_2 = PROCESS_TABLE_2
        self.PROCESS_TABLE_3 = PROCESS_TABLE_3
        self.PROCESS_TABLE_4 = PROCESS_TABLE_4
        self.PROCESS_TABLE_6 = PROCESS_TABLE_6

    def getSwitches(self):
        single_record_array = [self.PROCESS_TABLE_1, self.PROCESS_TABLE_2, self.PROCESS_TABLE_3 ,self.PROCESS_TABLE_4, self.PROCESS_TABLE_6]

        return single_record_array


class StreamToLogger(object):
   """
   Fake file-like stream object that redirects writes to a logger instance.
   """
   def __init__(self, logger, log_level=logging.INFO):
      self.logger = logger
      self.log_level = log_level
      self.linebuf = ''

   def write(self, buf):
      for line in buf.rstrip().splitlines():
         self.logger.log(self.log_level, line.rstrip())

logging.basicConfig(
   level=logging.DEBUG,
   format='%(asctime)s:%(levelname)s:%(name)s:%(message)s',
   filename="out.log",
   filemode='a'
)