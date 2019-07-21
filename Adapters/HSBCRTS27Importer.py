import csv
import glob
import os
import fnmatch
from pprint import pprint
from decimal import Decimal


from datetime import datetime

from openpyxl import load_workbook
from Modules import RTS27_Table_Records_Module
from Modules_DB_Readers import RTS27_Prod_Class_DB_Reader_Module
from Modules_DB_Readers import HistMktData_DB_Reader_Module
from Modules_DB_Writers import RTS27_DB_Writer_Module
from Utilities import RTS27_Utilities
from Modules import RTS27_LEI_Company_Map_Module

#
# PRIMARY Key to Join Tables 2,4,6 :
# Company Name/ID (HSBC, HBFR, HBPL etc)
# ISIN
# Trade Date
# CCY
#

hsbc_source_path = "/Users/sarthakagarwal/PycharmProjects/MifidDataAnalyser/Source/HSBC"
rts_db_rd = RTS27_Prod_Class_DB_Reader_Module.RTS27_Prod_Class_DB_Reader()
rtsdb = RTS27_DB_Writer_Module.RTS27_DB_Writer()
histdata_db_reader = HistMktData_DB_Reader_Module.HistMktData_DB_Reader()

hsbcfilenames = sorted(glob.glob(hsbc_source_path+'/*.csv'))

table_switches = RTS27_Utilities.RTS27_TableSwitches("N", "N", "Y", "N", "N") #Table 1, Table 2, Table 3, Table 4, Table 6


#filenames = filenames[0:len(filenames)
# hsbcfilenames = hsbcfilenames[0:3]
source_firm_group_name = "HSBC"
source_company_name = "HSBC"

print ("Array Length is" + str(len(hsbcfilenames)))
fileId = 0
list_of_table2_records = []
with open('HSBC_RTS27_TxResults.csv', 'w') as writeFile:
    writer = csv.writer(writeFile)

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

            if (("table3.csv" in str(filename)) and (table_switches.PROCESS_TABLE_3 == "Y")):
                print "Processing Table 3"
                readCSV = csv.reader(csvfile, delimiter=',')
                rowCount = 0

                for row in readCSV:
                    rowCount = rowCount + 1
                    if (rowCount > 1):
                        # Populate Table 3 properties
                        table3_rec_new = RTS27_Table_Records_Module.RTS27_Table3()
                        #  Table 2 Is just being created as a Helper Object to help extract the Currency Pair, Value Dates etc.
                        table2_rec = RTS27_Table_Records_Module.RTS27_Table2(rts_db_rd.getCfi_assetclass_map(),rts_db_rd.getCfi_char_map())

                        rawdate = datetime.strptime(row[0], '%d/%m/%Y')
                        formatted_date = datetime.strftime(rawdate, "%Y-%m-%d")

                        table3_rec_new.setInstrumentName(str(row[1]).decode('ascii', errors='ignore'))
                        table3_rec_new.setISIN(row[2])
                        table3_rec_new.setCurrency(row[3])

                        table3_rec_new.setSimpleAverageExecutedPrice(row[6])
                        table3_rec_new.setTotalValueExecuted(row[7])
                        table3_rec_new.setPrice(row[8])

                        table3_rec_new.setTimeOfExecution(row[9])
                        table3_rec_new.setTransactionSize(row[10])
                        table3_rec_new.setTradingSystem(row[11])
                        table3_rec_new.setTradingMode(row[12])
                        table3_rec_new.setTradingPlatform(row[13])
                        table3_rec_new.setBestBidOfferOrSuitableRef(row[14])

                        table3_rec_new.setSourceCompanyName(source_company_name)
                        table3_rec_new.setFileName(os.path.basename(filename))
                        table3_rec_new.setTradeDate(formatted_date)

                        table2_rec.setInstrumentName(table3_rec_new.INSTRUMENT_NAME)

                        if ((table3_rec_new.TIME_OF_EXECUTION!="") and (table3_rec_new.PRICE!='') and (table3_rec_new.PRICE!='0')
                                and ('Foreign_Exchange Forward' in table3_rec_new.INSTRUMENT_NAME)):

                            value_date = datetime.strptime(table2_rec.VALUE_DATE, '%Y-%m-%d')
                            trade_date = datetime.strptime(table3_rec_new.TRADE_DATE, '%Y-%m-%d')
                            tenor_days = value_date - trade_date

                            ccy_pair = (table2_rec.CCY_PAIR).strip()


                            if (min(table2_rec.CCY1.strip(),table2_rec.CCY2.strip()) == table3_rec_new.CURRENCY.strip()):
                                table3_rec_new.setConvertedPrice(table3_rec_new.PRICE)
                            else :
                                table3_rec_new.setConvertedPrice(1/float(table3_rec_new.PRICE))

                            fx_mid_price_for_ccy_tenor = histdata_db_reader.getFXFwdHistMktDataForCcy(ccy_pair,
                                                                                                      table3_rec_new.getTimeOfExecutionInESTWithoutDayLightSavings(),
                                                                                                      tenor_days)
                            table3_rec_new.setFileId(
                                source_company_name + "_" + table3_rec_new.TRADE_DATE + "_" + table3_rec_new.ISIN + "_" + table3_rec_new.CURRENCY)

                            if (fx_mid_price_for_ccy_tenor != 0):

                                #print "Comparison Results : " + str(tenor_days.days) + "," + str(table3_rec_new.TRANSACTION_SIZE) + "," + \
                                #      str(table3_rec_new.PRICE) + "," + str(fx_mid_price_for_ccy_tenor) + "," + \
                                #      str(format(abs(float(table3_rec_new.PRICE) - fx_mid_price_for_ccy_tenor),'.18f'))

                                table3_rec_new.setMidMarketRate(fx_mid_price_for_ccy_tenor)

                                table3_rec_new.setCurrencyPair(table2_rec.CCY_PAIR.strip())
                                table3_rec_new.setTenorDays(tenor_days.days)

                                abs_price_diff = abs(float(table3_rec_new.CONVERTED_PRICE) - fx_mid_price_for_ccy_tenor)
                                table3_rec_new.setAbsPriceDiff(str(format(abs_price_diff,'.18f')))

                                table3_rec_new.setMarkupAmount(abs_price_diff * float(table3_rec_new.TRANSACTION_SIZE))
                                table3_rec_new.setTCAPerformed()

                                if (table3_rec_new.CURRENCY.strip() != "USD"):
                                    usd_ccy_pair = table3_rec_new.CURRENCY.strip() + "USD"
                                    fx_spot_for_ccy = histdata_db_reader.getFXSpotHistMktDataForCcyPair(usd_ccy_pair,
                                                                                                              table3_rec_new.getTimeOfExecutionInESTWithoutDayLightSavings(),
                                                                                                              )
                                    markup_usd = float(fx_spot_for_ccy) * float(table3_rec_new.MARKUP_AMOUNT)

                                    table3_rec_new.setMarkUpUSD(markup_usd)

                                else :
                                    table3_rec_new.setMarkUpUSD(table3_rec_new.MARKUP_AMOUNT)

                                print (table3_rec_new.getAttrArrayTable())
                                #writer.writerow(table3_rec_new.getAttrArrayTable())

                            # Writing output of Table 6
                            # Create structure
                            rtsdb.Write_to_Table3(table3_rec_new)