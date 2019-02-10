import csv
import glob
import os
from datetime import datetime

from Modules import RTS27_Table_Records_Module
from Modules_DB_Readers import RTS27_Prod_Class_DB_Reader_Module
from Modules_DB_Writers import RTS27_DB_Writer_Module
from Utilities import RTS27_Utilities

#
# PRIMARY Key to Join Tables 2,4,6 :
# Company Name/ID (HSBC, HBFR, HBPL etc)
# ISIN
# Trade Date
# CCY
#

hsbc_source_path = "/Users/sarthakagarwal/PycharmProjects/MifidDataAnalyser/Source/HSBC"
rts_db_rd = RTS27_Prod_Class_DB_Reader_Module.RTS27_Prod_Class_DB_Reader()

hsbcfilenames = sorted(glob.glob(hsbc_source_path+'/*.csv'))
rtsdb = RTS27_DB_Writer_Module.RTS27_DB_Writer()

table_switches = RTS27_Utilities.RTS27_TableSwitches("Y", "Y", "Y", "Y") #Table 1, Table 2, Table 4, and Table 6

#filenames = filenames[0:len(filenames)
# hsbcfilenames = hsbcfilenames[0:3]
source_firm_group_name = "HSBC"
source_company_name = "HSBC"

print ("Array Length is" + str(len(hsbcfilenames)))
fileId = 0
list_of_table2_records = []
for filename in hsbcfilenames:
    print("Processing file" + filename)

    # Table1 does not have much info
    with open(filename) as csvfile:
        if (("table1.csv" in str(filename)) and (table_switches.PROCESS_TABLE_1=="Y")):

            readCSV = csv.reader(csvfile, delimiter=',')
            rowCount = 0

            for row in readCSV:
                rowCount = rowCount + 1
                if (rowCount > 1) :

                    # Printing output of Table 2
                    table1_rec = RTS27_Table_Records_Module.RTS27_Table1()
                    table1_rec.setSourceCompanyGroupName(source_firm_group_name)
                    table1_rec.setSourceCompanyName(source_company_name)
                    table1_rec.setSourceCompanyCode(row[1])
                    table1_rec.setCountryOfCompetentAuthority(row[2])
                    table1_rec.setMarketSegmentName(row[3])
                    table1_rec.setMarketSegmentID(row[4])
                    table1_rec.setTradeDate(row[5])
                    table1_rec.setOutagesNature(row[6])
                    table1_rec.setOutagesNumber(row[7])
                    table1_rec.setOutagesAverageDuration(row[8])
                    table1_rec.setScheduledAutionNature(row[9])
                    table1_rec.setScheduledAutionNumber(row[10])
                    table1_rec.setScheduledAutionAverageDuration(row[11])
                    table1_rec.setFailedTransactionsNumber(row[12])
                    table1_rec.setFailedTransactionsPercent(row[13])

                    table1_rec.setFileName(os.path.basename(filename))
                    table1_rec.setFileId(table1_rec.SOURCE_COMPANY_CODE + "_" + row[5])

                    # Writing output of Table 1
                    rtsdb.Write_to_Table1(table1_rec)

        if (("table2.csv" in str(filename)) and (table_switches.PROCESS_TABLE_2=="Y")):

            readCSV = csv.reader(csvfile, delimiter=',')
            rowCount = 0

            for row in readCSV:
                rowCount = rowCount + 1
                if (rowCount > 1) :

                    # Printing output of Table 2
                    table2_rec = RTS27_Table_Records_Module.RTS27_Table2(rts_db_rd.getCfi_assetclass_map(),rts_db_rd.getCfi_char_map())
                    table2_rec.setSourceCompanyName(source_company_name)
                    table2_rec.setFileName(os.path.basename(filename))
                    rawdate = datetime.strptime(row[0], '%d/%m/%Y')
                    formatted_date = datetime.strftime(rawdate, "%Y-%m-%d")
                    table2_rec.setTradeDate(formatted_date)
                    table2_rec.setInstrumentName(str(row[1]).decode('ascii', errors='ignore'))
                    table2_rec.setISIN(row[2])
                    table2_rec.setInstrumentClassification(row[4])
                    table2_rec.setCurrency(row[5])
                    table2_rec.setVenue("") # Since HSBC does not provide this Info...
                    table2_rec.setFileId(source_company_name+"_"+table2_rec.TRADE_DATE+"_"+table2_rec.ISIN+"_"+table2_rec.CURRENCY )

                    # Writing output of Table 2
                    rtsdb.Write_to_Table2(table2_rec)

        if (("table4.csv" in str(filename)) and (table_switches.PROCESS_TABLE_4=="Y")):
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
                    table4_rec_new.SOURCE_COMPANY_NAME = source_company_name
                    table4_rec_new.FILENAME = os.path.basename(filename)
                    table4_rec_new.TRADE_DATE = formatted_date
                    table4_rec_new.INSTRUMENT_NAME = str(row[1]).decode('ascii', errors='ignore')
                    table4_rec_new.ISIN = row[2]
                    table4_rec_new.CURRENCY = row[3]

                    table4_rec_new.setSimpleAverageTransactionPrice(str(row[4]))
                    table4_rec_new.VOLUME_WEIGHTED_TRANSACTION_PRICE = str(row[5])
                    table4_rec_new.HIGHEST_EXECUTED_PRICE = str(row[6])
                    table4_rec_new.LOWEST_EXECUTED_PRICE = str(row[7])

                    table4_rec_new.setFileId(source_company_name+"_"+table4_rec_new.TRADE_DATE+"_"+table4_rec_new.ISIN+"_"+table4_rec_new.CURRENCY )


                    # print table4_rec_new.getAttrArray()
                    # Writing to Table 4 to Database
                    rtsdb.Write_to_Table4(table4_rec_new)

        if (("table6.csv" in str(filename)) and (table_switches.PROCESS_TABLE_6=="Y")):
            readCSV = csv.reader(csvfile, delimiter=',')
            rowCount = 0

            for row in readCSV:
                rowCount = rowCount + 1
                if (rowCount > 1) :
                    # Populate Table 6 properties
                    table6_rec = RTS27_Table_Records_Module.RTS27_Table6()


                    rawdate = datetime.strptime(row[0], '%d/%m/%Y')
                    formatted_date = datetime.strftime(rawdate, "%Y-%m-%d")
                    table6_rec.setSourceCompanyName(source_company_name)
                    table6_rec.setFileName(os.path.basename(filename))
                    table6_rec.setTradeDate(formatted_date)
                    table6_rec.setInstrumentName(str(row[1]).decode('ascii', errors='ignore'))
                    table6_rec.setISIN(row[2])
                    table6_rec.setCurrency(row[3])
                    table6_rec.setNumberOfOrderOrRequestForQuote(row[4])
                    table6_rec.setNumberOfTransactionsExecuted(row[5])
                    table6_rec.setTotalValueOfTransactionsExecuted(row[6])
                    table6_rec.setNumberOfOrdersOrRequestCancelledOrWithdrawn(row[7])
                    table6_rec.setNumberOfOrdersOrRequestModified(row[8])
                    table6_rec.setMedianTransactionSize(row[9])
                    table6_rec.setMedianSizeOfAllOrdersOrRequestsForQuote(row[10])
                    table6_rec.setNumberOfDesignatedMarketMaker(row[11])

                    table6_rec.setFileId(source_company_name+"_"+table6_rec.TRADE_DATE+"_"+table6_rec.ISIN+"_"+table6_rec.CURRENCY)


                    # Writing output of Table 6
                    # Create structure
                    rtsdb.Write_to_Table6(table6_rec)


