import fnmatch
import os
from datetime import datetime
from openpyxl import load_workbook
from xml.dom import minidom
import xml.etree.ElementTree as ET
import ast, codecs
from Modules import RTS27_DB_Writer_Module, RTS27_Table_Records_Module, RTS27_Utilities

db_source_path = "/Users/sarthakagarwal/PycharmProjects/MifidDataAnalyser/Source/DeutscheBank/UnzippedSource"
rtsdb = RTS27_DB_Writer_Module.RTS27_DB_Writer()

dbfilenames = []
for root, dirnames, filenames in os.walk(db_source_path):
    for filename in fnmatch.filter(filenames, '*.xml'):
        dbfilenames.append(os.path.join(root, filename))

dbfilenames = dbfilenames[0:2]
source_firm_name = "DeutscheBankAG"

table_switches = RTS27_Utilities.RTS27_TableSwitches("N","N","N","Y") #Table 1, Table 2, Table 4, and Table 6

fileId = 0
list_of_table2_records = []
list_of_table4_records = []
list_of_table6_records = []
for filename in dbfilenames:
    print("Processing file" + filename)
    xml_file =  codecs.open(filename, 'r',encoding='utf-8')

    fileId = fileId + 1
    mydoc = minidom.parse(xml_file)

    financialInstruments = mydoc.getElementsByTagName('FinInstrument')

    trade_date = (mydoc.getElementsByTagName('DtTrdDay')[0].firstChild.data).encode('utf-8', errors='ignore')
    venue =  (mydoc.getElementsByTagName('MktSgmt')[0].firstChild.data).encode('utf-8', errors='ignore')

    for financialInstrument in financialInstruments :
        table2_rec = RTS27_Table_Records_Module.RTS27_Table2()
        table4_rec = RTS27_Table_Records_Module.RTS27_Table4()
        table6_rec = RTS27_Table_Records_Module.RTS27_Table6()

        ## -----------
        ## Building Table 2

        table2_rec.setTradeDate(trade_date)
        table2_rec.setSourceCompanyName(source_firm_name)
        table2_rec.setFileName(os.path.basename(filename))
        table2_rec.setVenue(venue)
        table2_rec.setFileId(source_firm_name + "_" + str(fileId))
        table2_rec.setISIN((financialInstrument.getElementsByTagName("FinInstr")[0].firstChild.data).encode('utf-8', errors='ignore'))
        table2_rec.setInstrumentName((financialInstrument.getElementsByTagName("FinInstrNm")[0].firstChild.data).encode('utf-8', errors='ignore'))
        table2_rec.setInstrumentClassification((financialInstrument.getElementsByTagName("CFICd")[0].firstChild.data).encode('utf-8', errors='ignore'))
        table2_rec.setCurrency((financialInstrument.getElementsByTagName("Ccy")[0].firstChild.data).encode('utf-8', errors='ignore'))

        ## -----------
        ## Building Table 4

        table4_rec.setTradeDate(trade_date)
        table4_rec.setSourceCompanyName(source_firm_name)
        table4_rec.setFileName(os.path.basename(filename))
        table4_rec.setISIN((financialInstrument.getElementsByTagName("FinInstr")[0].firstChild.data).encode('utf-8', errors='ignore'))
        table4_rec.setInstrumentName((financialInstrument.getElementsByTagName("FinInstrNm")[0].firstChild.data).encode('utf-8',errors='ignore'))
        if (len(financialInstrument.getElementsByTagName("SmplAvgTxPric")) > 0):
            table4_rec.setSimpleAverageTransactionPrice((financialInstrument.getElementsByTagName("SmplAvgTxPric")[0].firstChild.data).encode('utf-8', errors='ignore'))

        if (len(financialInstrument.getElementsByTagName("VolWghTxPric")) > 0):
            table4_rec.setVolumeWeightedTransactionPrice((financialInstrument.getElementsByTagName("VolWghTxPric")[0].firstChild.data).encode('utf-8', errors='ignore'))

        if (len(financialInstrument.getElementsByTagName("HgstTxPric")) > 0):
            table4_rec.setHighestExecutedPrice((financialInstrument.getElementsByTagName("HgstTxPric")[0].firstChild.data).encode('utf-8', errors='ignore'))

        if (len(financialInstrument.getElementsByTagName("LwstTxPric")) > 0):
            table4_rec.setLowestExecutedPrice((financialInstrument.getElementsByTagName("LwstTxPric")[0].firstChild.data).encode('utf-8', errors='ignore'))

        ## -----------
        ## Building Table 6
        table6_rec.setTradeDate(trade_date)
        table6_rec.setSourceCompanyName(source_firm_name)
        table6_rec.setFileName(os.path.basename(filename))
        table6_rec.setFileId(source_firm_name + "_" + str(fileId))
        table6_rec.setISIN(
            (financialInstrument.getElementsByTagName("FinInstr")[0].firstChild.data).encode('utf-8', errors='ignore'))
        table6_rec.setInstrumentName(
            (financialInstrument.getElementsByTagName("FinInstrNm")[0].firstChild.data).encode('utf-8',
                                                                                               errors='ignore'))

        if (len(financialInstrument.getElementsByTagName("NumRcvdOrQt")) > 0):
            table6_rec.setNumerOfOrderOrRequestForQuote((financialInstrument.getElementsByTagName("NumRcvdOrQt")[0].firstChild.data).encode('utf-8', errors='ignore'))

        if (len(financialInstrument.getElementsByTagName("NumExecTx")) > 0):
            table6_rec.setNumberOfTransactionsExecuted((financialInstrument.getElementsByTagName("NumExecTx")[0].firstChild.data).encode('utf-8', errors='ignore'))

        if (len(financialInstrument.getElementsByTagName("TtlValExecTx")) > 0):
            table6_rec.setTotalValueOfTransactionsExecuted((financialInstrument.getElementsByTagName("TtlValExecTx")[0].firstChild.data).encode('utf-8', errors='ignore'))

        if (len(financialInstrument.getElementsByTagName("NmOfCxlQrQt")) > 0):
            table6_rec.setNumberOfOrdersOrRequestCancelledOrWithdrawn((financialInstrument.getElementsByTagName("NmOfCxlQrQt")[0].firstChild.data).encode('utf-8', errors='ignore'))

        if (len(financialInstrument.getElementsByTagName("NmOfAmndQrQt")) > 0):
            table6_rec.setNumberOfOrdersOrRequestModified((financialInstrument.getElementsByTagName("NmOfAmndQrQt")[0].firstChild.data).encode('utf-8', errors='ignore'))

        if (len(financialInstrument.getElementsByTagName("MdnTxSize")) > 0):
            table6_rec.setMedianTransactionSize((financialInstrument.getElementsByTagName("MdnTxSize")[0].firstChild.data).encode('utf-8', errors='ignore'))

        if (len(financialInstrument.getElementsByTagName("MdnOrQtSize")) > 0):
            table6_rec.setMedianSizeOfAllOrdersOrRequestsForQuote((financialInstrument.getElementsByTagName("MdnOrQtSize")[0].firstChild.data).encode('utf-8', errors='ignore'))


        if (table_switches.PROCESS_TABLE_2 == "Y"):
            print (table2_rec.getAttrArray())

        if (table_switches.PROCESS_TABLE_4 == "Y"):
            print (table4_rec.getAttrArray())

        if (table_switches.PROCESS_TABLE_6 == "Y"):
            print (table6_rec.getAttrArray())

