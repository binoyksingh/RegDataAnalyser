import csv
import codecs
import glob
from datetime import datetime

from Modules import RTS27_DB_Writer_Module, RTS27_Table_Records_Module

hsbc_source_path = "/Users/sarthakagarwal/PycharmProjects/MifidDataAnalyser/Source/HSBC"
hsbcfilenames = sorted(glob.glob(hsbc_source_path+'/*.csv'))
rtsdb = RTS27_DB_Writer_Module.RTS27_DB_Writer()

PROCESS_TABLE_2="N"
PROCESS_TABLE_4="Y"
PROCESS_TABLE_6="N"


#filenames = filenames[0:len(filenames)
hsbcfilenames = hsbcfilenames[0:1000]
source_firm_name = "HSBC"

print ("Array Length is" + str(len(hsbcfilenames)))
fileId = 0
list_of_table2_records = []
for filename in hsbcfilenames:
    print("Processing file" + filename)

    # Table1 does not have much info
    with open(filename) as csvfile:
        if (("table2.csv" in str(filename)) and (PROCESS_TABLE_2=="Y")):
            readCSV = csv.reader(csvfile, delimiter=',')
            rowCount = 0

            for row in readCSV:
                rowCount = rowCount + 1
                if (rowCount > 1) :
                    # Printing output of Table 2
                    rawdate = datetime.strptime(row[0], '%d/%m/%Y')
                    formatted_date = datetime.strftime(rawdate, "%Y-%m-%d")
                    table2_fileIdString = source_firm_name + "_" + str(rowCount)
                    table2_instrumentname = str(row[1]).decode('ascii', errors='ignore')
                    table2_isin = row[2]
                    table2_instrumentclassification = row[4]
                    table2_currency = row[5]
                    table2_dateofthetradingday = formatted_date
                    table2_venue = "" # Since HSBC does not provide this Info...

                    # Writing output of Table 2
                    # Create structure
                    table2_rec = RTS27_Table_Records_Module.RTS27_Table2(source_firm_name, filename, table2_fileIdString, table2_isin, table2_dateofthetradingday, table2_venue, table2_instrumentname, table2_instrumentclassification, table2_currency)
                    #rtsdb.Write_to_Table2(table2_rec)

        if (("table4.csv" in str(filename)) and (PROCESS_TABLE_4=="Y")):
            readCSV = csv.reader(csvfile, delimiter=',')
            rowCount = 0

            for row in readCSV:
                rowCount = rowCount + 1
                if (rowCount > 1) :
                    # -----------------------------------------------------------
                    # Building Table 4
                    rawdate = datetime.strptime(row[0], '%d/%m/%Y')
                    formatted_date = datetime.strftime(rawdate, "%Y-%m-%d")
                    table4_rec_new = RTS27_Table_Records_Module.RTS27_Table4()
                    table4_rec_new.SOURCE_COMPANY_NAME = source_firm_name
                    table4_rec_new.FILENAME = filename
                    table4_rec_new.TRADE_DATE = formatted_date
                    table4_rec_new.FILE_ID = source_firm_name + "_" + str(rowCount)
                    table4_rec_new.INSTRUMENT_NAME = str(row[1]).decode('ascii', errors='ignore')
                    table4_rec_new.ISIN = row[2]
                    table4_rec_new.CURRENCY = row[3]

                    table4_rec_new.setSimpleAverageTransactionPrice(str(row[4]))
                    table4_rec_new.VOLUME_WEIGHTED_TRANSACTION_PRICE = str(row[5])
                    table4_rec_new.HIGHEST_EXECUTED_PRICE = str(row[6])
                    table4_rec_new.LOWEST_EXECUTED_PRICE = str(row[7])

                    # print table4_rec_new.getAttrArray()
                    # Writing to Table 4 to Database
                    rtsdb.Write_to_Table4(table4_rec_new)

        if (("table6.csv" in str(filename)) and (PROCESS_TABLE_6=="Y")):
            readCSV = csv.reader(csvfile, delimiter=',')
            rowCount = 0

            for row in readCSV:
                rowCount = rowCount + 1
                if (rowCount > 1) :
                    # Populate Table 6 properties
                    rawdate = datetime.strptime(row[0], '%d/%m/%Y')
                    formatted_date = datetime.strftime(rawdate, "%Y-%m-%d")
                    table6_dateofthetradingday = formatted_date
                    table6_fileIdString = source_firm_name + "_" + str(rowCount)
                    table6_instrumentname = str(row[1]).decode('ascii', errors='ignore')
                    table6_isin = row[2]
                    table6_currency = row[3]
                    table6_numberofordersorrequestforquotes = row[4]
                    table6_numberoftransactionsexecuted = row[5]
                    table6_totalvalueoftransactionsexecuted = row[6]
                    table6_numberoftransactionscancelledorwithdrawn = row[7]
                    table6_numberoforderorrequestmodified = row[8]
                    table6_mediantransactionsize = row[9]
                    table6_mediansizeofallordersorrequestsforquote = row[10]
                    table6_numberofdesignatedmarketmakers = row[11]

                    # Writing output of Table 6
                    # Create structure
                    table6_rec = RTS27_Table_Records_Module.RTS27_Table6(source_firm_name, filename, table6_fileIdString, table6_isin, table6_dateofthetradingday, table6_instrumentname,
                                                                         table6_numberofordersorrequestforquotes, table6_numberoftransactionsexecuted, table6_totalvalueoftransactionsexecuted,
                                                                         table6_numberoftransactionscancelledorwithdrawn, table6_numberoforderorrequestmodified, table6_mediantransactionsize,
                                                                         table6_mediansizeofallordersorrequestsforquote, table6_numberofdesignatedmarketmakers, table6_currency)
                    #rtsdb.Write_to_Table6(table6_rec)


