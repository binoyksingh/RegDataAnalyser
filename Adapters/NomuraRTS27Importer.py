#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 21 21:31:35 2018

@author: lojinilogesparan
"""

import csv
import os
from datetime import datetime
import RTS27_DB_Writer_Module
import RTS27_Table_Records_Module
rtsdb = RTS27_DB_Writer_Module.RTS27_DB_Writer()

filenameEOD = "/Users/lojinilogesparan/Documents/mifid_data/Nomura/Intl_EODReport-NIP_1q.csv"
source_firm_name = "Nomura International Plc"
filename = "EODReport-NIP_1q.csv"
dateformat =  '%d/%m/%Y' # '%m/%d/%Y for Bank NBI'


with open(filenameEOD, 'rb') as csvfile:
   data = csv.reader(csvfile, delimiter=',', quotechar='|')
   rowCount = 0
   for row in data:
       if rowCount > 0:
           print('Row='+str(rowCount))
           
           # ------------------------------
           # Building Table 2
           table2_rec = RTS27_Table_Records_Module.RTS27_Table2()
           rawdate = datetime.strptime(row[6],dateformat)
           formatted_date = datetime.strftime(rawdate, "%Y-%m-%d")
           table2_rec.setFileId(source_firm_name + "_" + str(0))
           table2_rec.setInstrumentName(str(row[3]))
           table2_rec.setISIN(str(row[2]))
           table2_rec.setInstrumentClassification(str(row[4]))
           table2_rec.setCurrency(str(row[5]))
           table2_rec.setSourceCompanyName(source_firm_name)
           table2_rec.setFileName(os.path.basename(filename))
           table2_rec.setTradeDate(formatted_date)
           table2_rec.setFileId(source_firm_name)
           
           #print table2_rec.getAttrArray()
           
           # Writing to Table 2
           rtsdb.Write_to_Table2(table2_rec)
            
           # ------------------------------
           # Building Table 4
           table4_rec_new = RTS27_Table_Records_Module.RTS27_Table4()
           table4_rec_new.SOURCE_COMPANY_NAME = source_firm_name
           table4_rec_new.FILENAME = filename
           table4_rec_new.TRADE_DATE = formatted_date
           table4_rec_new.FILE_ID = table2_rec.FILE_ID
           table4_rec_new.INSTRUMENT_NAME = table2_rec.INSTRUMENT_NAME
           table4_rec_new.ISIN = table2_rec.ISIN
           table4_rec_new.CURRENCY = table2_rec.CURRENCY
           
           if (row[7] != ' '):
               table4_rec_new.setSimpleAverageTransactionPrice(str(row[7]))
           if (row[8] != ' '):
               table4_rec_new.setVolumeWeightedTransactionPrice(str(row[8]))
           if (row[9] != ' '):
               table4_rec_new.setHighestExecutedPrice(str(row[9]))
           if (row[10] != ' '):
                table4_rec_new.setLowestExecutedPrice(str(row[10]))

           #print table4_rec_new.getAttrArray()
           # Writing to Table 4
           rtsdb.Write_to_Table4(table4_rec_new)
           
           # -----------------------------------------------------------
           # Building Table 6
           table6_rec_new = RTS27_Table_Records_Module.RTS27_Table6()
           table6_rec_new.SOURCE_COMPANY_NAME = source_firm_name
           table6_rec_new.FILENAME = filename
           table6_rec_new.TRADE_DATE = formatted_date
           table6_rec_new.FILE_ID = table2_rec.FILE_ID
           table6_rec_new.INSTRUMENT_NAME = table2_rec.INSTRUMENT_NAME
           table6_rec_new.ISIN = table2_rec.ISIN
           table6_rec_new.CURRENCY = table2_rec.CURRENCY
           
           if (row[11] != ' ' ) :
               table6_rec_new.setNumberOfOrderOrRequestForQuote(int(row[11]))
           if (row[12] != ' ' ) :
               table6_rec_new.setNumberOfTransactionsExecuted(int(row[12]))
           if (row[13] != ' ' ) :
               table6_rec_new.setTotalValueOfTransactionsExecuted(str(row[13]))
               
           if (row[14] != ' ' ) :
               table6_rec_new.setNumberOfOrdersOrRequestCancelledOrWithdrawn(int(row[14]))

           if (row[15] != ' ' ) :
               table6_rec_new.setNumberOfOrdersOrRequestModified(row[15])

           if (row[16] != ' ' ) :
               table6_rec_new.setMedianTransactionSize(str(row[16]))

           if (row[17] != ' ' ) :
               table6_rec_new.setMedianSizeOfAllOrdersOrRequestsForQuote(str(row[17]))

           if (row[18] != ' ' ) :
               table6_rec_new.setNumberOfDesignatedMarketMaker(str(row[18]))

           #print table6_rec_new.getAttrArray()

           # Writing to Table 6 to Database
           rtsdb.Write_to_Table6(table6_rec_new)
           
           
       rowCount += 1    
       