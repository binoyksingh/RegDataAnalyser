import csv
import glob
import os
import fnmatch
from Modules import RTS27_DB_Writer_Module, RTS27_Table_Records_Module, RTS27_Utilities
from Modules import RTS27_Prod_Class_DB_Reader_Module

citi_source_path = "/Users/sarthakagarwal/PycharmProjects/UnZippedSource2"
#filenames = sorted(glob.glob(hsbc_source_path+'/*.csv'))
rtsdb = RTS27_DB_Writer_Module.RTS27_DB_Writer()

citifilenames = []
for root, dirnames, filenames in os.walk(citi_source_path):
    for filename in fnmatch.filter(filenames, '*.csv'):
        citifilenames.append(os.path.join(root, filename))

#citifilenames = citifilenames[0:100]
rts_db_rd = RTS27_Prod_Class_DB_Reader_Module.RTS27_Prod_Class_DB_Reader()

table_switches = RTS27_Utilities.RTS27_TableSwitches("Y","Y","Y","Y") #Table 1, Table 2, Table 4, and Table 6

print ("Array Length is" + str(len(citifilenames)))
fileId = 0
for filename in citifilenames:
   fileId = fileId + 1
   source_firm_name="CITI"
   fileIdString = source_firm_name + "_" + str(fileId)
   print("Percentage Complete : " + str(round((float(fileId)/float(len(citifilenames)) * 100),2)) + "%")

   # Table1 properties
   table1_rec = RTS27_Table_Records_Module.RTS27_Table1()

   # Table 2 properties
   table2_rec = RTS27_Table_Records_Module.RTS27_Table2()

   # Table 4 properties
   table4_rec_new = RTS27_Table_Records_Module.RTS27_Table4()

   # Table 6 properties
   table6_rec = RTS27_Table_Records_Module.RTS27_Table6()

   with open(filename) as csvfile:
       readCSV = csv.reader(csvfile, delimiter=',')
       for row in readCSV:
           #print(row)
           #if ((row[6]=="Table 1:") and (row[7]is  not "Table 1 Start:") and (row[7] is not "Table 1 End:")) :
            if (len(row)>8):
                # Populate Table1 properties
                if (row[7] == "Investment Firm*"):
                    table1_rec.setSourceCompanyCode((str(row[8])).split(':')[1])
                    table1_rec.setSourceCompanyGroupName("CITI")

                if (row[7] == "Date of the Trading Day:" ):
                    table1_rec.setTradeDate(row[8])

                if (row[7] == "Venue Name:"):
                    table1_rec.setSourceCompanyName(row[8])

                if (row[7] == "Venue (ISO 10383 Market Identifier Code (MIC) or Legal Entity Identifier (LEI)):"):
                    table1_rec.setMarketSegmentID(row[8])

                if (row[7] == "Country of Competent Authority:"):
                    table1_rec.setCountryOfCompetentAuthority(row[8])

                if (row[7] == "Market Segment (ISO 10383 Market Segment MIC):"):
                    table1_rec.setMarketSegmentID(row[8])

                if (row[7] == "Market Segment:"):
                    table1_rec.setMarketSegmentName(row[8])

                if ("Instrument Identifier (ISO 6166):" in row[7] ):
                    table1_rec.setISIN(row[8])

                if ("Instrument Name:"  in row[7] ):
                    table1_rec.setInstrumentName(row[8])

                if ("Instrument Classification (ISO 10962 CFI Code):" in row[7]):
                    table1_rec.setInstrumentClassification(row[8])

                if ("Currency (ISO 4217):" in row[7]):
                    table1_rec.setCurrency(row[8])

                table1_rec.setFileName(os.path.basename(filename))
                fileIdString = table1_rec.SOURCE_COMPANY_CODE +"_"+table1_rec.ISIN +"_"+table1_rec.TRADE_DATE +"_"+ table1_rec.CURRENCY+"_"+str(fileId)
                table1_rec.setFileId(fileIdString)

                # Populate Table2 properties
                table2_rec.setSourceCompanyName(table1_rec.SOURCE_COMPANY_NAME)
                table2_rec.setFileName(os.path.basename(filename))
                table2_rec.setTradeDate(table1_rec.TRADE_DATE)

                if ("Instrument Identifier (ISO 6166):" in row[7] ):
                    table2_rec.setISIN(row[8])

                if ("Instrument Name:"  in row[7] ):
                    table2_rec.setInstrumentName(row[8])

                if ("Instrument Classification (ISO 10962 CFI Code):" in row[7]):
                    print ( os.path.basename(filename) +"_"+row[8])
                    table2_rec.setInstrumentClassification(row[8],rts_db_rd.getCfi_assetclass_map(), rts_db_rd.getCfi_char_map())

                if ("Currency (ISO 4217):" in row[7]):
                    table2_rec.setCurrency(row[8])

                table2_rec.setFileId(fileIdString)

                # Populate Table4 properties
                table4_rec_new.SOURCE_COMPANY_NAME = table1_rec.SOURCE_COMPANY_NAME
                table4_rec_new.FILENAME = os.path.basename(filename)
                table4_rec_new.TRADE_DATE = table1_rec.TRADE_DATE
                table4_rec_new.FILE_ID = fileIdString
                table4_rec_new.INSTRUMENT_NAME = table2_rec.INSTRUMENT_NAME
                table4_rec_new.ISIN = table2_rec.ISIN
                table4_rec_new.CURRENCY = table2_rec.CURRENCY

                if (row[7] == "Simple average transaction price" ):
                    table4_rec_new.setSimpleAverageTransactionPrice(row[8])

                if (row[7] == "Volume-weighted transaction price" ):
                    table4_rec_new.setVolumeWeightedTransactionPrice(row[8])

                if (row[7] == "Highest executed price" ):
                    table4_rec_new.setHighestExecutedPrice((row[8]))

                if (row[7] == "Lowest executed price" ):
                    table4_rec_new.setLowestExecutedPrice(row[8])

                # Populate Table 6 properties
                table6_rec.setSourceCompanyName(table1_rec.SOURCE_COMPANY_NAME)
                table6_rec.setFileId(fileIdString)
                table6_rec.setFileName(os.path.basename(filename))
                table6_rec.setTradeDate(table1_rec.TRADE_DATE)
                table6_rec.setISIN(table2_rec.ISIN)
                table6_rec.setCurrency(table2_rec.CURRENCY)
                table6_rec.setInstrumentName(table2_rec.INSTRUMENT_NAME)
                if ("Number Of Order or Request for Quotes" in row[7]):
                    table6_rec.setNumberOfOrderOrRequestForQuote(row[8])

                if ("Number Of Transactions Executed" in row[7]):
                    table6_rec.setNumberOfTransactionsExecuted(row[8])

                if ("Total Value of Transactions Executed" in row[7]): # the space here is quite important !!
                    table6_rec.setTotalValueOfTransactionsExecuted(row[8])

                if ("Number Of Order or Request Cancelled/Withdrawn" in row[7]):
                    table6_rec.setNumberOfOrdersOrRequestCancelledOrWithdrawn(row[8])

                if ("Number Of Order or Request Modified" in row[7]):
                    table6_rec.setNumberOfOrdersOrRequestModified(row[8])

                if ("Median Transaction Size" in row[7]):
                    table6_rec.setMedianTransactionSize(row[8])

                if ("Median Size of All Order or request for quotes" in row[7]):
                    table6_rec.setMedianSizeOfAllOrdersOrRequestsForQuote(row[8])

                if ("Number of Designated Market Maker" in row[7]):
                    table6_rec.setNumberOfDesignatedMarketMaker(row[8])

       # Printing output of Table 1
       if(table_switches.PROCESS_TABLE_1 == "Y"):
            # print (table2_rec.getAttrArray())
            rtsdb.Write_to_Table1(table1_rec)

       # Printing output of Table 2
       if(table_switches.PROCESS_TABLE_2 == "Y"):
            #print (table2_rec.getAttrArray())
            rtsdb.Write_to_Table2(table2_rec)

       # Printing output of Table 4
       if(table_switches.PROCESS_TABLE_4 == "Y"):
           rtsdb.Write_to_Table4(table4_rec_new)

       # Printing output of Table 6
       if(table_switches.PROCESS_TABLE_6 == "Y"):
            # print (table6_rec.getAttrArray())
            rtsdb.Write_to_Table6(table6_rec)



