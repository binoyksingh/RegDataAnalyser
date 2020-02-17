#!/usr/bin/python

from Modules_DB_Writers.Generic_DB_Writer_Module import Generic_DB_Writer

class FILE_PROCESS_STATUS_DB_Writer(Generic_DB_Writer):

    def getInsertSQLString(self):

        insert_sql_string = "INSERT INTO `FILE_PROCESS_STATUS` (`FILE_NAME`,`FILE_TYPE`,`FILE_STATUS`,`START_TIMESTAMP`,`FINISH_TIMESTAMP`) VALUES (%s, %s, %s, %s, %s)"
        return insert_sql_string

    def getFileProcessStatus(self, filename):

        check_file_status_sql = "SELECT FILE_STATUS from FILE_PROCESS_STATUS where FILE_NAME in (%s)"

        self.cursor.execute(check_file_status_sql, filename)

        myresult = self.cursor.fetchall()
        file_status = ""
        for x in myresult:
            file_status = x[0]

        return file_status


