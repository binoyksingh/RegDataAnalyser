#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sun Aug 26 21:58:22 2018

@author: lojinilogesparan
"""

import csv
import os
from datetime import datetime
import RTS27_DB_Writer_Module
import RTS27_Table_Records_Module
rtsdb = RTS27_DB_Writer_Module.RTS27_DB_Writer()
import RTS27_Prod_Class_DB_Reader_Module
rts_db_rd = RTS27_Prod_Class_DB_Reader_Module.RTS27_Prod_Class_DB_Reader()

dateformat =  '%Y-%m-%d'
newRecordCaptured = -1
rowCount = 0   
newTradeID = 0
source_firm_group_name = 'JPMorgan'

path = '/Users/lojinilogesparan/Documents/mifid_data/JPM/JPMorgan-ExecutionVenueReporting-RTS27-Q1-2018/'

for foldername in os.listdir(path):
    if os.path.isdir(path+'/'+foldername):
        mainfilename = path+'/'+foldername
        for subfoldername in os.listdir(mainfilename):
            if os.path.isdir(mainfilename+'/'+subfoldername):
                source_firm_name = foldername+'-'+subfoldername

                # Loading data
                # ------------------------------
                subpath = mainfilename+'/'+subfoldername
                for filename in os.listdir(subpath):
                    filenameEOD = subpath + '/' + filename
                    print('Loading file = ' + filenameEOD)
                    
                    with open(filenameEOD, 'rb') as csvfile:
                       data = csv.reader(csvfile, delimiter=',', quotechar='|')    
                       
                       for row in data:
                           
                           if len(row) >= 1:
                               if row[0] == 'Date of the trading day':
                                   rawdate = datetime.strptime(row[1],dateformat)
                           
                           if len(row) > 2:
                               if row[1] == 'Financial Instrument':
                                   
                                   # New record generation
                                   # ------------------------------
                                   newRecordCaptured += 1
                                   newTradeID += 1
                                   
                                   # Building Table 2
                                   table2_rec = RTS27_Table_Records_Module.RTS27_Table2()   
                                   formatted_date = datetime.strftime(rawdate, "%Y-%m-%d")
                                   table2_rec.setTradeDate(formatted_date)
                                   table2_rec.setInstrumentName(str(row[2]))
                                   table2_rec.setISIN(str(row[3]))
                                   table2_rec.setSourceCompanyName(source_firm_name)
                                   table2_rec.setFileName(os.path.basename(filename))
                                   
                                   # ------------------------------
                                   # Building Table 4
                                   table4_rec_new = RTS27_Table_Records_Module.RTS27_Table4()
                                   table4_rec_new.SOURCE_COMPANY_NAME = source_firm_name
                                   table4_rec_new.FILENAME = filename
                                   table4_rec_new.TRADE_DATE = formatted_date
                                                      
                                   # -----------------------------------------------------------
                                   # Building Table 6
                                   table6_rec_new = RTS27_Table_Records_Module.RTS27_Table6()
                                   table6_rec_new.SOURCE_COMPANY_NAME = source_firm_name
                                   table6_rec_new.FILENAME = filename
                                   table6_rec_new.TRADE_DATE = formatted_date
                
                               # Table 1
                               if row[0]=='Venue':
                                   # Building Table 1 object
                                   table1_rec = RTS27_Table_Records_Module.RTS27_Table1()
                                   table1_rec.setFileName(os.path.basename(filename))
                                   table1_rec.setSourceCompanyGroupName(source_firm_group_name)
                                   table1_rec.setSourceCompanyName(str(row[1]))
                                   table1_rec.setSourceCompanyCode(str(row[2]))
                               if row[0] == 'Country of Competent Authority':
                                   table1_rec.setCountryOfCompetentAuthority(str(row[1]))
                               if row[0] == 'Market Segment':
                                   table1_rec.setMarketSegmentName(row[1])
                                   table1_rec.setMarketSegmentID(row[2])
                                   if row[1] != '' or row[2] != '' or row[3] != '':
                                       # CHECK DATA
                                       print(row)
                               if row[0] == 'Outages':
                                   table1_rec.setOutagesNature(row[1])
                                   table1_rec.setOutagesNumber(row[2])
                                   table1_rec.setOutagesAverageDuration(row[3])
                                   if row[1] != '' or row[2] != '' or row[3] != '':
                                       # CHECK DATA
                                       print(row)
                               if row[0] == 'Scheduled Auction':
                                   table1_rec.setScheduledAutionNature(row[1])
                                   table1_rec.setScheduledAutionNumber(row[2])
                                   table1_rec.setScheduledAutionAverageDuration(row[3])
                                   if row[1] != '' or row[2] != '' or row[3] != '':
                                       # CHECK DATA
                                       print(row)
                               if row[0] == 'Failed Transactions':
                                   table1_rec.setFailedTransactionsNumber(row[2])
                                   table1_rec.setFailedTransactionsPercent(row[3])
                               
                               # Table 2
                               if row[1] == 'Instrument classification':
                                   table2_rec.setInstrumentClassification(str(row[2]), rts_db_rd.getCfi_assetclass_map(), rts_db_rd.getCfi_char_map())
                               if row[1] == 'Currency':
                                   table2_rec.setCurrency(str(row[2]))
                               
                               # Table 4   
                               if (row[2] == 'Simple average transaction price'):
                                   tmpValue = row[3]
                                   table4_rec_new.setSimpleAverageTransactionPrice(str(tmpValue[:tmpValue.find(' ')]))
                               if (row[2] == 'Volume-weighted transaction price'):
                                   tmpValue = row[3]
                                   table4_rec_new.setVolumeWeightedTransactionPrice(str(tmpValue[:tmpValue.find(' ')]))
                               if (row[2] == 'Highest executed price'):
                                   tmpValue = row[3]
                                   table4_rec_new.setHighestExecutedPrice(str(tmpValue[:tmpValue.find(' ')]))
                               if (row[2] == 'Lowest executed price'):
                                   tmpValue = row[3]
                                   table4_rec_new.setLowestExecutedPrice(str(tmpValue[:tmpValue.find(' ')]))
                                    
                               # Table 6           
                               if (row[2] == 'Number of orders or request for quotes received' ) :
                                   if row[3] != '':
                                       table6_rec_new.setNumberOfOrderOrRequestForQuote(int(row[3]))
                                   else:
                                       table6_rec_new.setNumberOfOrderOrRequestForQuote(0)
                               if (row[2] == 'Number of transactions executed' ) :
                                   if row[3] != '':
                                       table6_rec_new.setNumberOfTransactionsExecuted(int(row[3]))
                                   else:
                                       table6_rec_new.setNumberOfTransactionsExecuted(0)
                               if (row[2] == 'Total value of transactions executed' ) :
                                   if row[3] != '':
                                       tmpValue = row[3]
                                       table6_rec_new.setTotalValueOfTransactionsExecuted(str(tmpValue[:tmpValue.find(' ')]))   
                                   else:
                                       table6_rec_new.setTotalValueOfTransactionsExecuted(str(' '))  
                               if (row[2] == 'Number of orders or request for quotes received cancelled or withdrawn' ) :
                                   if row[3] != '':
                                       table6_rec_new.setNumberOfOrdersOrRequestCancelledOrWithdrawn(int(row[3]))
                                   else:
                                       table6_rec_new.setNumberOfOrdersOrRequestCancelledOrWithdrawn(0)
                               if (row[2] == 'Number of orders or request for quotes received modified' ) :
                                   if row[3] != '':
                                       table6_rec_new.setNumberOfOrdersOrRequestModified(row[3])
                                   else:
                                       table6_rec_new.setNumberOfOrdersOrRequestModified(' ')
                               if (row[2] == 'Median transaction size' ) :
                                   if row[3] != '':
                                       tmpValue = row[3]
                                       table6_rec_new.setMedianTransactionSize(str(tmpValue[:tmpValue.find(' ')]))
                                   else:
                                       table6_rec_new.setMedianTransactionSize(str(' '))
                               if (row[2] == 'Median size of all orders or requests for quote' ) :
                                   if row[3] != '':
                                       tmpValue = row[3]
                                       table6_rec_new.setMedianSizeOfAllOrdersOrRequestsForQuote(str(tmpValue[:tmpValue.find(' ')]))
                                   else:
                                       table6_rec_new.setMedianSizeOfAllOrdersOrRequestsForQuote(str(' ')) 
                               if (row[2] == 'Number of designated market makers' ) :
                                   if row[3] != '':
                                       table6_rec_new.setNumberOfDesignatedMarketMaker(str(row[3]))
                                   else:
                                       table6_rec_new.setNumberOfDesignatedMarketMaker(str(' '))
                                   
                               if row[2] == 'Median time elapsed between request and provision of any corresponding quotes':
                                   # Store previous record data
                                   table2_rec.setFileId(table1_rec.SOURCE_COMPANY_CODE + "_" + formatted_date+ "_" + str(newTradeID))
                           
                                   # ------------------------------
                                   # Writing to Table 1
                                   table1_rec.setTradeDate(formatted_date)
                                   table1_rec.ISIN = table2_rec.ISIN
                                   table1_rec.INSTRUMENT_CLASSIFICATION = table2_rec.INSTRUMENT_CLASSIFICATION
                                   table1_rec.INSTRUMENT_NAME = table2_rec.INSTRUMENT_NAME
                                   table1_rec.CURRENCY = table2_rec.CURRENCY
                                   table1_rec.FILE_ID = table2_rec.FILE_ID
                                   #print table1_rec.getAttrArrayTable1()
                                   rtsdb.Write_to_Table1(table1_rec)
                                   
                                   # Writing to Table 2
                                   #print table2_rec.getAttrArray()
                                   rtsdb.Write_to_Table2(table2_rec)
                                   
                                   # Writing to Table 4
                                   table4_rec_new.FILE_ID = table2_rec.FILE_ID
                                   table4_rec_new.INSTRUMENT_NAME = table2_rec.INSTRUMENT_NAME
                                   table4_rec_new.ISIN = table2_rec.ISIN
                                   table4_rec_new.CURRENCY = table2_rec.CURRENCY
                                   #print table4_rec_new.getAttrArray()
                                   rtsdb.Write_to_Table4(table4_rec_new)
                                   
                                   # Writing to Table 6 to Database
                                   table6_rec_new.FILE_ID = table2_rec.FILE_ID
                                   table6_rec_new.INSTRUMENT_NAME = table2_rec.INSTRUMENT_NAME
                                   table6_rec_new.ISIN = table2_rec.ISIN
                                   table6_rec_new.CURRENCY = table2_rec.CURRENCY
                                   #print table6_rec_new.getAttrArray()
                                   rtsdb.Write_to_Table6(table6_rec_new)
                                   
                           rowCount += 1                 
                           
rtsdb.__del__()