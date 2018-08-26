import csv
import codecs
import glob
import os
import fnmatch
import logging
import sys
from datetime import datetime
from Modules import RTS27_DB_Writer_Module, RTS27_Table_Records_Module, RTS27_Utilities

def checkHeader (header):
    header_tokens = header[0].split(",")
    all_cols_present = True
    col_names_required = ["Date of the trading day",
                            "Financial Instrument / Name",
                          "Financial Instrument / Identifier (ISIN)",
                          "Instrument Classification",
                          "Currency",
                          "Market Segment / Identifier",
                          "simple average transaction price",
                          "weighted transaction price",
                          "lowest executed price",
                          "highest executed price",
                          "Number of orders or request for quotes received",
                          "Number of transactions executed",
                          "Total value of transactions executed",
                          "Number of orders or request for quotes received cancelled or withdrawn",
                          "Number of orders or request for quotes received modified",
                          "Median transaction size",
                          "Median size of all orders or requests for quote",
                          "Number of designated market makers"]

    for requiredcol in col_names_required :
        if (requiredcol not in header) :
            all_cols_present = False
    return all_cols_present

gsi_source_path = "/Users/sarthakagarwal/PycharmProjects/MifidDataAnalyser/Source/GoldmanSachs/UnzippedFiles"
path_to_log_directory = "/Users/sarthakagarwal/PycharmProjects/MifidDataAnalyser/Source/GoldmanSachs/ResultsLog"
log_filename = "gsi_log.txt"

logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
rootLogger = logging.getLogger()

fileHandler = logging.FileHandler(filename=os.path.join(path_to_log_directory, log_filename))
fileHandler.setFormatter(logFormatter)
rootLogger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)

# [TO-DO] : GET THE LOGGER WORKING so STDOUT is added to
#stdout_logger = logging.getLogger('STDOUT')
#sl = RTS27_Utilities.StreamToLogger(rootLogger, logging.INFO)
#sys.stdout = sl

rtsdb = RTS27_DB_Writer_Module.RTS27_DB_Writer()

table_switches = RTS27_Utilities.RTS27_TableSwitches("N","Y","Y","Y") #Table 1, Table 2, Table 4, and Table 6

gsifilenames = []
for root, dirnames, filenames in os.walk(gsi_source_path):
    for filename in fnmatch.filter(filenames, '*.txt'):
        gsifilenames.append(os.path.join(root, filename))

gsifilenames.sort()
#gsifilenames = gsifilenames[0:3]
source_firm_name = "Goldman Sachs International"

logging.info("Array Length is" + str(len(gsifilenames)))
fileId = 0
list_of_table2_records = []
for filename in gsifilenames:
    logging.info("Processing file" + filename)
    fileId = fileId + 1

    # Table1 does not have much info
    with open(filename) as csvfile:
        readCSV = csv.reader(csvfile, delimiter='|')
        rowCount = 0

        logging.info("Percentage Complete : " + str(round((float(fileId) / float(len(gsifilenames)) * 100), 2)) + "%")

        for row in readCSV:
            rowCount = rowCount + 1
            if (rowCount == 1) :
                # Assuming the first row is header
                gsi_headers = row
                # check if all headers are there, otherwise skip the file and log it.
                if (checkHeader(gsi_headers) == False) :
                    logging.info ("All columns are not present - skipping file :" + filename)
                    break;

            if (rowCount > 1) :

                table2_rec = RTS27_Table_Records_Module.RTS27_Table2()
                table4_rec = RTS27_Table_Records_Module.RTS27_Table4()
                table6_rec = RTS27_Table_Records_Module.RTS27_Table6()

                # Printing output of Table 2
                rawdate = datetime.strptime(row[gsi_headers.index("Date of the trading day")], '%Y-%m-%d')
                formatted_date = datetime.strftime(rawdate, "%Y-%m-%d")
                table2_rec.setSourceCompanyName(source_firm_name)
                table2_rec.setTradeDate(formatted_date)
                table2_rec.setFileId(source_firm_name + "_" + str(rowCount))
                table2_rec.setFileName(os.path.basename(filename))
                table2_rec.setInstrumentName(str(row[gsi_headers.index("Financial Instrument / Name")]))
                table2_rec.setISIN(row[gsi_headers.index("Financial Instrument / Identifier (ISIN)")])
                table2_rec.setInstrumentClassification(row[gsi_headers.index("Instrument Classification")])
                table2_rec.setCurrency(row[gsi_headers.index("Currency")])
                table2_rec.setVenue(row[gsi_headers.index("Market Segment / Identifier")])

                # -----------------------------------------------------------
                # Building Table 4
                table4_rec.setSourceCompanyName(source_firm_name)
                table4_rec.setFileName(os.path.basename(filename))
                table4_rec.setTradeDate(formatted_date)
                table4_rec.setFileId(source_firm_name + "_" + str(rowCount))
                table4_rec.setInstrumentName(str(row[gsi_headers.index("Financial Instrument / Name")]))
                table4_rec.setISIN(row[gsi_headers.index("Financial Instrument / Identifier (ISIN)")])
                table4_rec.setSimpleAverageTransactionPrice(str(row[gsi_headers.index("simple average transaction price")]))
                table4_rec.setVolumeWeightedTransactionPrice(str(row[gsi_headers.index("weighted transaction price")]))
                table4_rec.setHighestExecutedPrice(str(row[gsi_headers.index("highest executed price")]))
                table4_rec.setLowestExecutedPrice(str(row[gsi_headers.index("lowest executed price")]))
                table4_rec.setCurrency(row[gsi_headers.index("Currency")])


                # Populate Table 6 properties
                table6_rec.setSourceCompanyName(source_firm_name)
                table6_rec.setTradeDate(formatted_date)
                table6_rec.setFileId(source_firm_name + "_" + str(rowCount))
                table6_rec.setFileName(os.path.basename(filename))
                table6_rec.setInstrumentName(str(row[gsi_headers.index("Financial Instrument / Name")]))
                table6_rec.setISIN(row[gsi_headers.index("Financial Instrument / Identifier (ISIN)")])
                table6_rec.setCurrency(row[gsi_headers.index("Currency")])

                table6_rec.setNumberOfOrderOrRequestForQuote(row[gsi_headers.index("Number of orders or request for quotes received")])
                table6_rec.setNumberOfTransactionsExecuted(row[gsi_headers.index("Number of transactions executed")])
                table6_rec.setTotalValueOfTransactionsExecuted(row[gsi_headers.index("Total value of transactions executed")])
                table6_rec.setNumberOfOrdersOrRequestCancelledOrWithdrawn(row[gsi_headers.index("Number of orders or request for quotes received cancelled or withdrawn")])
                table6_rec.setNumberOfOrdersOrRequestModified(row[gsi_headers.index("Number of orders or request for quotes received modified")])
                table6_rec.setMedianTransactionSize(row[gsi_headers.index("Median transaction size")])
                table6_rec.setMedianSizeOfAllOrdersOrRequestsForQuote(row[gsi_headers.index("Median size of all orders or requests for quote")])
                table6_rec.setNumberOfDesignatedMarketMaker(row[gsi_headers.index("Number of designated market makers")])

                #print (table6_rec.getAttrArray())
                if (table_switches.PROCESS_TABLE_2 == "Y"):
                # print (table2_rec.getAttrArray())
                    rtsdb.Write_to_Table2(table2_rec)

                if (table_switches.PROCESS_TABLE_4 == "Y"):
                # print (table4_rec.getAttrArray())
                    rtsdb.Write_to_Table4(table4_rec)

                if (table_switches.PROCESS_TABLE_6 == "Y"):
                # print (table6_rec.getAttrArray())
                    rtsdb.Write_to_Table6(table6_rec)
