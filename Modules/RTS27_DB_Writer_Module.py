#!/usr/bin/python

import pymysql, sys
import RTS27_Table_Records_Module

class RTS27_DB_Writer:

    BATCH_SIZE = 1000
    connection = pymysql.Connection

    def __init__(self):
        print ('RTS27_DB_WRITER:INIT:Calling Constructor')
        self.connection = pymysql.connect(host='35.224.67.19', user='root', password='root', db='mifid')
        self.cursor = self.connection.cursor()

        self.list_of_table2_records = []
        self.list_of_table6_records = []
        print ('RTS27_DB_WRITER:INIT:Connection Success')

    def __del__(self):
        print ('RTS27_DB_WRITER:INIT:Calling destructor')
        if (len(self.list_of_table2_records)!=0):
            print ("still something left in the kitty... ")
            # Insert Batch
            batch = []
            for rec in self.list_of_table2_records:
                #single_record_array = [rec.SOURCE_COMPANY_NAME, rec.FILENAME, rec.FILE_ID, rec.ISIN, rec.TRADE_DATE,
                #                       rec.VENUE, rec.INSTRUMENT_NAME, rec.INSTRUMENT_CLASSIFICATION, rec.CURRENCY]
                batch.append(rec.getAttrArray)
            self.Write_to_Table2_DB(batch)
            self.list_of_table2_records = []

        if (len(self.list_of_table6_records)!=0):
            print ("SOme table6 records left..")
            # Insert Batch
            table6_batch = []
            for table6_rec in self.list_of_table6_records:
                table6_batch.append(table6_rec.getAttrArray())
            self.Write_to_Table6_DB(table6_batch)
            self.list_of_table6_records = []

        self.connection.close()

    # Write data to RTS27 Table 2
    def Write_to_Table2(self, table_2_record):
        self.list_of_table2_records.append(table_2_record)
        if (len(self.list_of_table2_records) == self.BATCH_SIZE) :
            # Insert Batch
            batch = []
            for rec in self.list_of_table2_records:
                #single_record_array = [rec.SOURCE_COMPANY_NAME, rec.FILENAME,rec.FILE_ID, rec.ISIN, rec.TRADE_DATE, rec.VENUE, rec.INSTRUMENT_NAME, rec.INSTRUMENT_CLASSIFICATION, rec.CURRENCY ]
                single_record_array = rec.getAttrArray()
                batch.append(single_record_array)
            self.Write_to_Table2_DB(batch)
            self.list_of_table2_records = []

    def Write_to_Table2_DB(self, batch):

        # Printing output of Table 2
        insert_rts27__table2_sql_string = "INSERT INTO `MIFID_RTS27_TABLE2` (`SOURCE_COMPANY_NAME`, `FILENAME`,`FILE_ID`,`ISIN`,`TRADE_DATE`,`VENUE`," \
                                          "`INSTRUMENT_NAME`,`INSTRUMENT_CLASSIFICATION`,`CURRENCY`) " \
                                          " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s )"

        #self.cursor.execute(insert_rts27__table2_sql_string, (publishing_firm_name, filename, fileIdString, table2_isin, table2_dateofthetradingday, table2_venue, table2_instrumentname, table2_instrumentclassification,table2_currency))
        self.cursor.executemany(insert_rts27__table2_sql_string, batch)
        self.connection.commit()
        #print(publishing_firm_name + ";" + fileIdString + ";" + table2_isin + ";" + table2_dateofthetradingday + ";" + table2_venue + ";" + table2_instrumentname + ";" + table2_instrumentclassification + ";" + table2_currency)
        return;

# Write data to RTS27 Table 4

# Write data to RTS27 Table 6
    def Write_to_Table6(self, table_6_record):
        self.list_of_table6_records.append(table_6_record)
        if (len(self.list_of_table6_records) == self.BATCH_SIZE) :
            # Insert Batch
            batch = []
            for rec in self.list_of_table6_records:
                batch.append(rec.getAttrArray())
            self.Write_to_Table6_DB(batch)
            self.list_of_table6_records = []

    def Write_to_Table6_DB(self, batch):

        insert_rts27__table6_sql_string = "INSERT INTO `MIFID_RTS27_TABLE6` (`SOURCE_COMPANY_NAME`, `FILENAME`,`FILE_ID`,`ISIN`,`TRADE_DATE`," \
                                          "`INSTRUMENT_NAME`,`NUMBER_OF_ORDER_OR_REQUEST_FOR_QUOTE`,`NUMBER_OF_TRANSACTIONS_EXECUTED`," \
                                          "`TOTAL_VALUE_OF_TRANSACTIONS_EXECUTED`,`NUMBER_OF_ORDERS_OR_REQUEST_CANCELLED_OR_WITHDRAWN`," \
                                          "`NUMBER_OF_ORDERS_OR_REQUEST_MODIFIED`,`MEDIAN_TRANSACTION_SIZE`,`MEDIAN_SIZE_OF_ALL_ORDERS_OR_REQUESTS_FOR_QUOTE`," \
                                          "`NUMBER_OF_DESIGNATED_MARKET_MAKER`,`CURRENCY`) " \
                                          " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"

        self.cursor.executemany(insert_rts27__table6_sql_string, batch)
        self.connection.commit()
        return;


