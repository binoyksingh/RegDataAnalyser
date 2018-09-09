#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sat Sep  8 13:51:21 2018

@author: lojinilogesparan
"""

import os
from datetime import datetime
import RTS27_DB_Writer_Module
import RTS27_Table_Records_Module
rtsdb = RTS27_DB_Writer_Module.RTS27_DB_Writer()

path = "/Users/lojinilogesparan/Documents/mifid_data/ubs/ubs ag/Jan18/localstorage/dlprod/apps/be-engine/Jan2018"
source_firm_name = "UBS AG London Branch"
dateformat =  '%Y-%m-%d' 

fileCount = 0
for filename in os.listdir(path):
    if len(filename) >= 12:
        if filename[:11].isalnum() and filename[12].isalnum()==False and filename.find('rfqId')==-1 and filename.find('quote')==-1:
            print(filename)
            fileCount += 1
            
            table2start = 0
            table4start = 0
            table6start = 0
            with open(path+'/'+filename, 'rb') as inputfile:
                
                # Create tables
                table2_rec = RTS27_Table_Records_Module.RTS27_Table2()
                table4_rec_new = RTS27_Table_Records_Module.RTS27_Table4()
                table6_rec_new = RTS27_Table_Records_Module.RTS27_Table6()
                
                # trading date
                rawdate = datetime.strptime(filename[-18:-8],dateformat)
                formatted_date = datetime.strftime(rawdate, "%Y-%m-%d")
                
                for line in inputfile:
                    # Determine start and end of relevant tables
                    if line.find('table2')>0:
                        table2start += 1
                            
                    elif line.find('table4')>0:    
                        table4start += 1
                        
                    elif line.find('table6')>0:
                        table6start += 1
                            

                    # Start writing to arrays
                    if table2start == 1:
                        # ------------------------------
                        # Building Table 2
                        if line.find('FinancialInstrumentName')>0:
                            table2_rec.setInstrumentName(line[line.find('"')+1:line.find('/')-1])
                        if line.find('ISIN')>0:
                            table2_rec.setISIN(line[line.find('ISIN')+6:line.find('ISIN')+18])
                        if line.find('InstrumentClassification')> 0:
                            table2_rec.setInstrumentClassification(line[line.find('"')+1:line.find('/')-1])
                        if line.find('Currency')>0:
                            table2_rec.setCurrency(line[line.find('>')+1:line.find('/')-1])
                    
                    
                    elif table4start == 1:
                        # ------------------------------
                        # Building Table 4
                        if line.find('simpleAveragePrice')>0:
                            if line.find('insufficientData="true"')>0:
                                table4_rec_new.setSimpleAverageTransactionPrice('')
                            else:
                                table4_rec_new.setSimpleAverageTransactionPrice(line[line.find('>')+1:line.find('</')-1])
                        
                        if line.find('volumeWeightedAveragePrice')>0:
                            if line.find('insufficientData="true"')>0:
                                table4_rec_new.setVolumeWeightedTransactionPrice('')
                            else:
                                table4_rec_new.setVolumeWeightedTransactionPrice(line[line.find('>')+1:line.find('</')-1])
                        
                        if line.find('highestExecutedPrice')>0:
                            if line.find('insufficientData="true"')>0:
                                table4_rec_new.setHighestExecutedPrice('')
                            else:
                                table4_rec_new.setHighestExecutedPrice(line[line.find('>')+1:line.find('</')-1])
                        
                        if line.find('lowestExecutedPrice')>0:
                            if line.find('insufficientData="true"')>0:
                                table4_rec_new.setLowestExecutedPrice('')
                            else:
                                table4_rec_new.setLowestExecutedPrice(line[line.find('>')+1:line.find('</')-1])
                        
                    elif table6start == 1:
                        # -----------------------------------------------------------
                        # Building Table 6                
                        if line.find('numberOrdersAndRfqsReceived')>0:
                            table6_rec_new.setNumberOfOrderOrRequestForQuote(line[line.find('>')+1:line.find('</')-1])
                        if line.find('numberTransactionsExecuted')>0:
                            table6_rec_new.setNumberOfTransactionsExecuted(line[line.find('>')+1:line.find('</')-1])
                        if line.find('totalValueTransactionsExecuted')>0:
                            table6_rec_new.setTotalValueOfTransactionsExecuted(line[line.find('>')+1:line.find('</')-1])
                        if line.find('numberOrdersAndRfqsReceivedCancelledWithdrawn')>0:
                            table6_rec_new.setNumberOfOrdersOrRequestCancelledOrWithdrawn(line[line.find('>')+1:line.find('</')-1])
                        if line.find('numberOrdersAndRfqsReceivedModified')>0:
                            table6_rec_new.setNumberOfOrdersOrRequestModified(line[line.find('>')+1:line.find('</')-1])
                        if line.find('medianTransactionSize')>0:
                            table6_rec_new.setMedianTransactionSize(line[line.find('>')+1:line.find('</')-1])
                        if line.find('medianOrderAndRfqSize')>0:
                            table6_rec_new.setMedianSizeOfAllOrdersOrRequestsForQuote(line[line.find('>')+1:line.find('</')-1])
                        if line.find('numberDesignatedMarketMakers')>0:
                            table6_rec_new.setNumberOfDesignatedMarketMaker(line[line.find('>')+1:line.find('</')-1])
                
                    
                    # When data gathering is complete
                    if line.find('</qoeReport>') >= 0:
                        # -----------------------------------------------------------
                        # Complete Table 2
                        table2_rec.setFileId(source_firm_name + "_" + str(0)) 
                        table2_rec.setSourceCompanyName(source_firm_name)
                        table2_rec.setFileName(os.path.basename(filename))
                        table2_rec.setFileId(source_firm_name) 
                        table2_rec.setTradeDate(formatted_date)
                        
                        print table2_rec.getAttrArray()
                        # Writing to Table 2
                        rtsdb.Write_to_Table2(table2_rec)

                        # -----------------------------------------------------------
                        # Complete Table 4
                        table4_rec_new.SOURCE_COMPANY_NAME = source_firm_name
                        table4_rec_new.FILENAME = filename
                        table4_rec_new.FILE_ID = ''
                        table4_rec_new.INSTRUMENT_NAME = table2_rec.INSTRUMENT_NAME
                        table4_rec_new.ISIN = table2_rec.ISIN
                        table4_rec_new.CURRENCY =  table2_rec.CURRENCY
                        table4_rec_new.TRADE_DATE = formatted_date
                        
                        print table4_rec_new.getAttrArray()
                        # Writing to Table 4
                        rtsdb.Write_to_Table4(table4_rec_new)
                        
                        # -----------------------------------------------------------
                        # Complete Table 6
                        table6_rec_new.SOURCE_COMPANY_NAME = source_firm_name
                        table6_rec_new.FILENAME = filename
                        table6_rec_new.TRADE_DATE = formatted_date
                        table6_rec_new.FILE_ID = ''
                        table6_rec_new.INSTRUMENT_NAME = table2_rec.INSTRUMENT_NAME
                        table6_rec_new.ISIN = table2_rec.ISIN
                        table6_rec_new.CURRENCY = table2_rec.CURRENCY
                        
                        print table6_rec_new.getAttrArray()
                        # Writing to Table 6 to Database
                        rtsdb.Write_to_Table6(table6_rec_new)

rtsdb.__del__()