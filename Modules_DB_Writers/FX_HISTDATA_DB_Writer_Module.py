#!/usr/bin/python

import pymysql, sys
import ConfigParser

class FX_HIST_DATA_DB_WRITER:

    BATCH_SIZE = 5000
    connection = pymysql.Connection
    config = ConfigParser.ConfigParser()

    def __init__(self):
        print ('FX_HIST_DATA_DB_WRITER:INIT:Calling Constructor')

        self.config.read("../Config/DatabaseConfig.txt")

        print (self.config.get("DATABASE_DETAILS","DATABASE_HOST"))

        DB_HOSTNAME=self.config.get("DATABASE_DETAILS","DATABASE_HOST")
        DB_USER=self.config.get("DATABASE_DETAILS","DATABASE_USER")
        DB_PASSWORD=self.config.get("DATABASE_DETAILS","DATABASE_PASSWORD")
        DB_NAME=self.config.get("DATABASE_DETAILS","DATABASE_NAME")

        self.list_of_fx_hist_records = []

        self.connection = pymysql.connect(host=DB_HOSTNAME, user=DB_USER, password=DB_PASSWORD, db=DB_NAME)
        self.cursor = self.connection.cursor()

        print ('FX_HIST_DATA_DB_WRITER:INIT:Connection Success')

    def __del__(self):
        print ('FX_HIST_DATA_DB_WRITER:INIT:Calling destructor')
        if (len(self.list_of_fx_hist_records)!=0):
            print ("Some records left..")
            # Insert Batch
            batch = []
            for rec in self.list_of_fx_hist_records:
                batch.append(rec.getAttrArray())
            self.Write_to_FX_HISTDATA_DB(batch)
            self.list_of_fx_hist_records = []
        self.connection.close()


    # Write data to RTS27 Table 4
    def Write_FX_HISTDATA(self, table_4_record):
        self.list_of_fx_hist_records.append(table_4_record)
        if (len(self.list_of_fx_hist_records) == self.BATCH_SIZE) :
            # Insert Batch
            batch = []
            for rec in self.list_of_fx_hist_records:
                batch.append(rec.getAttrArray())
            self.Write_to_FX_HISTDATA_DB(batch)
            self.list_of_fx_hist_records = []

    def Write_to_FX_HISTDATA_DB(self, batch):

        insert_fx_histdata_sql_string = "INSERT INTO `FX_HIST_DATA` (`CURRENCY_CODE`, `CURRENCY_NAME`,`BASE_CURRENCY_CODE`,`BASE_CURRENCY_NAME`,`UNITS_PER_BASE_CCY`," \
                                              "`RATE_DATE`) VALUES (%s, %s, %s, %s, %s, %s )"
        self.cursor.executemany(insert_fx_histdata_sql_string, batch)
        self.connection.commit()
        print ("committing deets")
        return;


