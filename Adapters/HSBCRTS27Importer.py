import csv
import codecs
import glob
import os
from datetime import datetime
from Modules import RTS27_Prod_Class_DB_Reader_Module

from Modules import RTS27_DB_Writer_Module, RTS27_Table_Records_Module

hsbc_source_path = "/Users/sarthakagarwal/PycharmProjects/MifidDataAnalyser/Source/HSBC"
rts_db_rd = RTS27_Prod_Class_DB_Reader_Module.RTS27_Prod_Class_DB_Reader()

hsbcfilenames = sorted(glob.glob(hsbc_source_path+'/*.csv'))
rtsdb = RTS27_DB_Writer_Module.RTS27_DB_Writer()

PROCESS_TABLE_2="Y"
PROCESS_TABLE_4="N"
PROCESS_TABLE_6="N"


#filenames = filenames[0:len(filenames)
hsbcfilenames = hsbcfilenames[0:3]
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
                    table2_rec = RTS27_Table_Records_Module.RTS27_Table2()
                    table2_rec.setSourceCompanyName(source_firm_name)
                    table2_rec.setFileName(os.path.basename(filename))
                    rawdate = datetime.strptime(row[0], '%d/%m/%Y')
                    formatted_date = datetime.strftime(rawdate, "%Y-%m-%d")
                    table2_rec.setFileId(source_firm_name + "_" + str(rowCount))
                    table2_rec.setInstrumentName(str(row[1]).decode('ascii', errors='ignore'))
                    table2_rec.setISIN(row[2])
                    table2_rec.setInstrumentClassification(row[4],rts_db_rd.getCfi_assetclass_map(),rts_db_rd.getCfi_char_map())
                    table2_rec.setCurrency(row[5])
                    table2_rec.setTradeDate(formatted_date)
                    table2_rec.setVenue("") # Since HSBC does not provide this Info...

                    print ("isda:" , table2_rec.ISDA_ASSET_CLASS_DESC, table2_rec.INSTRUMENT_CLASSIFICATION)
                    #print ("cfi:" , table2_rec.CFI_ATTR_1_DESC, table2_rec.CFI_ATTR_2_DESC,table2_rec.CFI_ATTR_3_DESC, table2_rec.CFI_ATTR_4_DESC, table2_rec.CFI_ATTR_5_DESC, table2_rec.CFI_ATTR_6_DESC)
                    # Writing output of Table 2
                    # table2_rec = RTS27_Table_Records_Module.RTS27_Table2(source_firm_name, filename, table2_fileIdString, table2_isin, table2_dateofthetradingday, table2_venue, table2_instrumentname, table2_instrumentclassification, table2_currency)
                    rtsdb.Write_to_Table2(table2_rec)

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
                    table6_rec = RTS27_Table_Records_Module.RTS27_Table6()


                    rawdate = datetime.strptime(row[0], '%d/%m/%Y')
                    formatted_date = datetime.strftime(rawdate, "%Y-%m-%d")
                    table6_rec.setSourceCompanyName(source_firm_name)
                    table6_rec.setFileName(os.path.basename(filename))
                    table6_rec.setTradeDate(formatted_date)
                    table6_rec.setFileId(source_firm_name + "_" + str(rowCount))
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

                    # Writing output of Table 6
                    # Create structure
                    rtsdb.Write_to_Table6(table6_rec)


