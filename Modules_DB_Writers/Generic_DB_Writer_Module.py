#!/usr/bin/python

import pymysql, sys
import ConfigParser

class Generic_DB_Writer:

    BATCH_SIZE = 5000
    connection = pymysql.Connection
    config = ConfigParser.ConfigParser()

    def __init__(self):
        self.config.read("../Config/DatabaseConfig.txt")

        DB_HOSTNAME=self.config.get("DATABASE_DETAILS","DATABASE_HOST")
        DB_USER=self.config.get("DATABASE_DETAILS","DATABASE_USER")
        DB_PASSWORD=self.config.get("DATABASE_DETAILS","DATABASE_PASSWORD")
        DB_NAME=self.config.get("DATABASE_DETAILS","DATABASE_NAME")

        self.list_of_generic_records = []

        self.connection = pymysql.connect(host=DB_HOSTNAME, user=DB_USER, password=DB_PASSWORD, db=DB_NAME)
        self.cursor = self.connection.cursor()

    def __del__(self):
        print ('Generic_DB_Writer:INIT:Calling destructor')
        if (len(self.list_of_generic_records)!=0):
            print ("Some records left..")
            # Insert Batch
            batch = []
            for rec in self.list_of_generic_records:
                batch.append(rec.getAttrArray())
            self.Write_to_DB(batch)
            self.list_of_generic_records = []
        self.connection.close()


    # Write data
    def Write_Data(self, table_record):
        self.list_of_generic_records.append(table_record)
        if (len(self.list_of_generic_records) == self.BATCH_SIZE) :
            # Insert Batch
            batch = []
            for rec in self.list_of_generic_records:
                batch.append(rec.getAttrArray())
            self.Write_to_DB(batch)
            self.list_of_generic_records = []

    def Write_to_DB(self, batch):
        insert_sql_string = self.getInsertSQLString()
        self.cursor.executemany(insert_sql_string, batch)
        self.connection.commit()
        return;


