#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Sep  3 22:29:12 2018

@author: lojinilogesparan
"""

import csv
import os
from datetime import datetime
import RTS27_DB_Writer_Module
import RTS27_Table_Records_Module
rtsdb = RTS27_DB_Writer_Module.RTS27_DB_Writer()

path = "/Users/lojinilogesparan/Documents/mifid_data/State Street Bank Intl/BESTEX_RTS27_ZMHGNT7ZPKZ3UFZ8EO46_2018Q1/"
source_firm_name = "State Street Bank Intl"
dateformat =  '%Y%m%d' 

for foldername in os.listdir(path):
    if foldername[0] != '.':
        for filename in os.listdir(path+'/'+foldername):
                filenameEOD = path+'/'+foldername+'/'+filename
                
                with open(filenameEOD, 'rb') as csvfile:
                    data = csv.reader(csvfile, delimiter=',', quotechar='|')
                    rowCount = 0
                    for row in data:
                        if rowCount > 0:
                            print('Row='+str(rowCount))
                       
                            if foldername == 'TABLE2':
                               # ------------------------------
                               # Building Table 2
                               table2_rec = RTS27_Table_Records_Module.RTS27_Table2()
                               table2_rec.setFileId(source_firm_name + "_" + str(0)) 
                               table2_rec.setInstrumentName(str(row[0]))
                               table2_rec.setISIN(str(row[1]))
                               table2_rec.setInstrumentClassification(str(row[3]))
                               table2_rec.setCurrency(str(row[4]))
                               table2_rec.setSourceCompanyName(source_firm_name)
                               table2_rec.setFileName(os.path.basename(filename))
                               table2_rec.setTradeDate('')
                               table2_rec.setFileId(source_firm_name) 
                               
                               print table2_rec.getAttrArray()
                               # Writing to Table 2
                               # rtsdb.Write_to_Table2(table2_rec)
                               
                            elif foldername == 'TABLE4':
                               # ------------------------------
                               # Building Table 4
                               table4_rec_new = RTS27_Table_Records_Module.RTS27_Table4()
                               table4_rec_new.SOURCE_COMPANY_NAME = source_firm_name
                               table4_rec_new.FILENAME = filename
                               table4_rec_new.FILE_ID = ''
                               table4_rec_new.INSTRUMENT_NAME = ''
                               table4_rec_new.ISIN = row[0]
                               table4_rec_new.CURRENCY = row[1]
                               
                               if (row[2] != ' '):
                                   table4_rec_new.setSimpleAverageTransactionPrice(str(row[2]))
                               if (row[3] != ' '):
                                   table4_rec_new.setVolumeWeightedTransactionPrice(str(row[3]))
                               if (row[4] != ' '):
                                   table4_rec_new.setHighestExecutedPrice(str(row[4]))
                               if (row[5] != ' '):
                                    table4_rec_new.setLowestExecutedPrice(str(row[5]))
                               
                               rawdate = datetime.strptime(filename[-12:-4],dateformat)
                               formatted_date = datetime.strftime(rawdate, "%Y-%m-%d")
                               table4_rec_new.TRADE_DATE = formatted_date
                               
                               print table4_rec_new.getAttrArray()
                               # Writing to Table 4
                               #rtsdb.Write_to_Table4(table4_rec_new)
                            
                            elif foldername == 'TABLE6':
               
                               # -----------------------------------------------------------
                               # Building Table 6
                               table6_rec_new = RTS27_Table_Records_Module.RTS27_Table6()
                               table6_rec_new.SOURCE_COMPANY_NAME = source_firm_name
                               table6_rec_new.FILENAME = filename
                               
                               rawdate = datetime.strptime(filename[-12:-4],dateformat)
                               formatted_date = datetime.strftime(rawdate, "%Y-%m-%d")
                               table6_rec_new.TRADE_DATE = formatted_date
                               
                               table6_rec_new.FILE_ID = ''
                               table6_rec_new.INSTRUMENT_NAME = ''
                               table6_rec_new.ISIN = row[0]
                               table6_rec_new.CURRENCY = ''
                               
                               if (row[1] != ' ' ) :
                                   table6_rec_new.setNumberOfOrderOrRequestForQuote(int(row[1]))
                               if (row[2] != ' ' ) :
                                   table6_rec_new.setNumberOfTransactionsExecuted(int(row[2]))
                               if (row[3] != ' ' ) :
                                   table6_rec_new.setTotalValueOfTransactionsExecuted(str(row[3]))
                               if (row[4] != ' ' ) :
                                   table6_rec_new.setNumberOfOrdersOrRequestCancelledOrWithdrawn(int(row[4]))
                               if (row[5] != ' ' ) :
                                   table6_rec_new.setNumberOfOrdersOrRequestModified(row[5])
                               if (row[6] != ' ' ) :
                                   table6_rec_new.setMedianTransactionSize(str(row[6]))
                               if (row[7] != ' ' ) :
                                   table6_rec_new.setMedianSizeOfAllOrdersOrRequestsForQuote(str(row[7]))
                               if (row[8] != ' ' ) :
                                   table6_rec_new.setNumberOfDesignatedMarketMaker(str(row[8]))
                    
                               print table6_rec_new.getAttrArray()
                               # Writing to Table 6 to Database
                               # rtsdb.Write_to_Table6(table6_rec_new)
                               
                        rowCount += 1
                        
rtsdb.__del__()