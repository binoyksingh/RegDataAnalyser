#!/usr/bin/python

import pymysql, sys
from decimal import Decimal

class RTS27_TableSwitches:

    PROCESS_TABLE_1 = "N"
    PROCESS_TABLE_2 = "N"
    PROCESS_TABLE_4 = "N"
    PROCESS_TABLE_6 = "N"

    def __init__(self, PROCESS_TABLE_1, PROCESS_TABLE_2,PROCESS_TABLE_4 , PROCESS_TABLE_6):

        self.PROCESS_TABLE_1 = PROCESS_TABLE_1
        self.PROCESS_TABLE_2 = PROCESS_TABLE_2
        self.PROCESS_TABLE_4 = PROCESS_TABLE_4
        self.PROCESS_TABLE_6 = PROCESS_TABLE_6

    def getSwitches(self):
        single_record_array = [self.PROCESS_TABLE_1, self.PROCESS_TABLE_2, self.PROCESS_TABLE_4, self.PROCESS_TABLE_6]

        return single_record_array
