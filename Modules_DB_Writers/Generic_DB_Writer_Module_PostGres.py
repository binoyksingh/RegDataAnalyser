#!/usr/bin/python

import sys
import ConfigParser
import psycopg2
import psycopg2.extras


class Generic_DB_Writer_Postgres:

    BATCH_SIZE = 5000
    connection = psycopg2._connect
    config = ConfigParser.ConfigParser()

    def __init__(self):
        self.config.read("../Config/DatabaseConfig.txt")

        DB_HOSTNAME_POSTGRES=self.config.get("DATABASE_DETAILS","DATABASE_HOST_POSTGRES")
        DB_PORT_POSTGRES=self.config.get("DATABASE_DETAILS","DATABASE_PORT_POSTGRES")
        DB_USER_POSTGRES=self.config.get("DATABASE_DETAILS","DATABASE_USER_POSTGRES")
        DB_PASSWORD_POSTGRES=self.config.get("DATABASE_DETAILS","DATABASE_PASSWORD_POSTGRES")
        DB_NAME_POSTGRES=self.config.get("DATABASE_DETAILS","DATABASE_NAME_POSTGRES")

        self.list_of_generic_records = []
        self.connection = psycopg2.connect(host=DB_HOSTNAME_POSTGRES, user=DB_USER_POSTGRES, password=DB_PASSWORD_POSTGRES, dbname=DB_NAME_POSTGRES)

        self.connection.set_session(autocommit=True)

        self.cursor = self.connection.cursor()

    def __del__(self):
        print ('Generic_DB_Writer_Postgres:INIT:Calling destructor')
        if (len(self.list_of_generic_records)!=0):
            # Insert Batch
            batch = []
            for rec in self.list_of_generic_records:
                batch.append(rec.getAttrArray())
            self.Write_to_PostGresDB(batch)
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
            self.Write_to_PostGresDB(batch)
            self.list_of_generic_records = []

    def Write_to_PostGresDB(self, batch):
        insert_sql_string = self.getInsertSQLString()
        psycopg2.extras.execute_values(self.cursor,insert_sql_string, batch)


