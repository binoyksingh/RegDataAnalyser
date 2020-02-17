#!/usr/bin/python

import sys

class File_Process_Status:

    # Define list of attributez
    FILE_NAME=""
    FILE_TYPE=""
    FILE_STATUS=""
    START_TIME = ""
    FINISHED_TIME = ""

    def setFileName(self, FILE_NAME):
        self.FILE_NAME = FILE_NAME

    def setFileType(self, FILE_TYPE):
        self.FILE_TYPE = FILE_TYPE

    def setFileStatus(self, FILE_STATUS):
        self.FILE_STATUS = FILE_STATUS

    def setStartTime(self, START_TIME):
        self.START_TIME = START_TIME

    def setFinishedTime(self, FINISHED_TIME):
        self.FINISHED_TIME = FINISHED_TIME

    def getAttrArray(self):
        single_record_array = [self.FILE_NAME,
                               self.FILE_TYPE,
                               self.FILE_STATUS,
                               self.START_TIME,
                               self.FINISHED_TIME
                               ]
        return single_record_array