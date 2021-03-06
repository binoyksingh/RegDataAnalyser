import fnmatch
import os
import xml.etree.ElementTree as ET

from Modules import RTS27_Table_Records_Module
from Modules_DB_Readers import RTS27_Prod_Class_DB_Reader_Module
from Modules_DB_Writers import RTS27_DB_Writer_Module
from Utilities import RTS27_Utilities


def processHeader (buffer) :
    headerXML = ET.fromstring(buffer)
    print (headerXML.findall('DtTrdDay')[0].text)

    trade_date = (headerXML.findall('DtTrdDay')[0].text).encode('utf-8', errors='ignore')
    venue =  (headerXML.findall('MktSgmt')[0].text).encode('utf-8', errors='ignore')

def processFinancialInstrument (buffer, header_str, rtsdb, rts_db_rd,fileId) :

    headerXML = ET.fromstring(header_str)

    trade_date = (headerXML.findall('DtTrdDay')[0].text).encode('utf-8', errors='ignore')
    source_firm_name = (headerXML.findall('VnNm')[0].text).encode('utf-8', errors='ignore')
    venue_code = (headerXML.findall('VnCd')[0].text).encode('utf-8', errors='ignore')
    country_of_competent_authority = (headerXML.findall('CtryCompAuth')[0].text).encode('utf-8', errors='ignore')
    market_segement_code = (headerXML.findall('MktSgmt')[0].text).encode('utf-8', errors='ignore')
    market_segement_name = (headerXML.findall('MktSgmtNm')[0].text).encode('utf-8', errors='ignore')

    venue = (headerXML.findall('MktSgmt')[0].text).encode('utf-8', errors='ignore')
    financialInstrument = ET.fromstring(buffer)

    table1_rec = RTS27_Table_Records_Module.RTS27_Table1()
    table2_rec = RTS27_Table_Records_Module.RTS27_Table2(rts_db_rd.getCfi_assetclass_map(), rts_db_rd.getCfi_char_map())
    table4_rec = RTS27_Table_Records_Module.RTS27_Table4()
    table6_rec = RTS27_Table_Records_Module.RTS27_Table6()

    ## -----------
    ## Building Table 1
    table1_rec.setSourceCompanyName(source_firm_name)
    table1_rec.setSourceCompanyGroupName(source_group_name)
    table1_rec.setSourceCompanyCode(venue_code)
    table1_rec.setCountryOfCompetentAuthority(country_of_competent_authority)
    table1_rec.setMarketSegmentName(market_segement_name)
    table1_rec.setMarketSegmentID(market_segement_code)
    table1_rec.setTradeDate(trade_date)
    table1_rec.setOutagesNumber(0) # DB doesnt provide this value
    table1_rec.setScheduledAutionNature(0) # DB doesnt provide this value
    table1_rec.setScheduledAutionNumber(0) # DB doesnt provide this value
    table1_rec.setFailedTransactionsNumber(0) # DB doesnt provide this value
    table1_rec.setCurrency((financialInstrument.findall("Ccy")[0].text).encode('utf-8', errors='ignore'))
    table1_rec.setISIN((financialInstrument.findall("FinInstr")[0].text).encode('utf-8', errors='ignore'))
    table1_rec.setInstrumentName((financialInstrument.findall("FinInstrNm")[0].text).encode('utf-8', errors='ignore'))
    table1_rec.setFileName(os.path.basename(filename))
    table1_rec.setInstrumentClassification((financialInstrument.findall("CFICd")[0].text).encode('utf-8', errors='ignore'))
    fileIdStr = table1_rec.SOURCE_COMPANY_CODE+"_"+table1_rec.MARKET_SEGMENT_ID+"_"+table1_rec.TRADE_DATE+"_"+table1_rec.ISIN+"_"+table1_rec.CURRENCY
    table1_rec.setFileId(fileIdStr)

    ## -----------
    ## Building Table 2
    table2_rec.setTradeDate(trade_date)
    table2_rec.setSourceCompanyName(source_firm_name)
    table2_rec.setFileName(os.path.basename(filename))
    table2_rec.setVenue(venue)
    table2_rec.setFileId(fileIdStr)
    table2_rec.setISIN(
        (financialInstrument.findall("FinInstr")[0].text).encode('utf-8', errors='ignore'))
    table2_rec.setInstrumentName(
        (financialInstrument.findall("FinInstrNm")[0].text).encode('utf-8', errors='ignore'))
    table2_rec.setInstrumentClassification(
        (financialInstrument.findall("CFICd")[0].text).encode('utf-8', errors='ignore'))
    table2_rec.setCurrency(
        (financialInstrument.findall("Ccy")[0].text).encode('utf-8', errors='ignore'))

    ## -----------
    ## Building Table 4

    table4_rec.setTradeDate(trade_date)
    table4_rec.setSourceCompanyName(source_firm_name)
    table4_rec.setFileName(os.path.basename(filename))
    table4_rec.setFileId(fileIdStr)

    table4_rec.setISIN(
        (financialInstrument.findall("FinInstr")[0].text).encode('utf-8', errors='ignore'))
    table4_rec.setInstrumentName(
        (financialInstrument.findall("FinInstrNm")[0].text).encode('utf-8', errors='ignore'))
    if (len(financialInstrument.findall("DaylyPric/SmplAvgTxPric")) > 0):
        table4_rec.setSimpleAverageTransactionPrice(
            (financialInstrument.findall("DaylyPric/SmplAvgTxPric")[0].text).encode('utf-8',
                                                                                                  errors='ignore'))
    table4_rec.setCurrency(
        (financialInstrument.findall("Ccy")[0].text).encode('utf-8', errors='ignore'))

    if (len(financialInstrument.findall("DaylyPric/VolWghTxPric")) > 0):
        table4_rec.setVolumeWeightedTransactionPrice(
            (financialInstrument.findall("DaylyPric/VolWghTxPric")[0].text).encode('utf-8',
                                                                                                 errors='ignore'))

    if (len(financialInstrument.findall("DaylyPric/HgstTxPric")) > 0):
        table4_rec.setHighestExecutedPrice(
            (financialInstrument.findall("DaylyPric/HgstTxPric")[0].text).encode('utf-8',
                                                                                               errors='ignore'))

    if (len(financialInstrument.findall("DaylyPric/LwstTxPric")) > 0):
        table4_rec.setLowestExecutedPrice(
            (financialInstrument.findall("DaylyPric/LwstTxPric")[0].text).encode('utf-8',
                                                                                               errors='ignore'))

    ## -----------
    ## Building Table 6

    table6_rec.setTradeDate(trade_date)
    table6_rec.setSourceCompanyName(source_firm_name)
    table6_rec.setFileName(os.path.basename(filename))
    table6_rec.setFileId(fileIdStr)
    table6_rec.setISIN(
        (financialInstrument.findall("FinInstr")[0].text).encode('utf-8', errors='ignore'))

    table6_rec.setInstrumentName(
        (financialInstrument.findall("FinInstrNm")[0].text).encode('utf-8',errors='ignore'))

    if (len(financialInstrument.findall("LkhdExec/NumRcvdOrQt")) > 0):
        table6_rec.setNumberOfOrderOrRequestForQuote(
            (financialInstrument.findall("LkhdExec/NumRcvdOrQt")[0].text).encode('utf-8',errors='ignore'))

    if (len(financialInstrument.findall("LkhdExec/NumExecTx")) > 0):
        table6_rec.setNumberOfTransactionsExecuted(
            (financialInstrument.findall("LkhdExec/NumExecTx")[0].text).encode('utf-8', errors='ignore'))

    if (len(financialInstrument.findall("LkhdExec/TtlValExecTx")) > 0):
        table6_rec.setTotalValueOfTransactionsExecuted(
            (financialInstrument.findall("LkhdExec/TtlValExecTx")[0].text).encode('utf-8',errors='ignore'))

    if (len(financialInstrument.findall("LkhdExec/NmOfCxlQrQt")) > 0):
        table6_rec.setNumberOfOrdersOrRequestCancelledOrWithdrawn(
            (financialInstrument.findall("LkhdExec/NmOfCxlQrQt")[0].text).encode('utf-8',
                                                                                                errors='ignore'))

    if (len(financialInstrument.findall("LkhdExec/NmOfAmndQrQt")) > 0):
        table6_rec.setNumberOfOrdersOrRequestModified(
            (financialInstrument.findall("LkhdExec/NmOfAmndQrQt")[0].text).encode('utf-8',
                                                                                                 errors='ignore'))

    if (len(financialInstrument.findall("LkhdExec/MdnTxSize")) > 0):
        table6_rec.setMedianTransactionSize(
            (financialInstrument.findall("LkhdExec/MdnTxSize")[0].text).encode('utf-8', errors='ignore'))

    if (len(financialInstrument.findall("LkhdExec/MdnOrQtSize")) > 0):
        table6_rec.setMedianSizeOfAllOrdersOrRequestsForQuote(
            (financialInstrument.findall("LkhdExec/MdnOrQtSize")[0].text).encode('utf-8',
                                                                                                errors='ignore'))
    table6_rec.setCurrency(
        (financialInstrument.findall("Ccy")[0].text).encode('utf-8', errors='ignore'))

    if (table_switches.PROCESS_TABLE_1 == "Y"):
        rtsdb.Write_to_Table1(table1_rec)

    if (table_switches.PROCESS_TABLE_2 == "Y"):
        rtsdb.Write_to_Table2(table2_rec)

    if (table_switches.PROCESS_TABLE_4 == "Y"):
        rtsdb.Write_to_Table4(table4_rec)

    if (table_switches.PROCESS_TABLE_6 == "Y"):
        rtsdb.Write_to_Table6(table6_rec)


db_source_path = "/Users/sarthakagarwal/PycharmProjects/MifidDataAnalyser/Source/DeutscheBank/UnzippedSource"
rtsdb = RTS27_DB_Writer_Module.RTS27_DB_Writer()
rts_db_rd = RTS27_Prod_Class_DB_Reader_Module.RTS27_Prod_Class_DB_Reader()

dbfilenames = []
for root, dirnames, filenames in os.walk(db_source_path):
    for filename in fnmatch.filter(filenames, '*.xml'):
        dbfilenames.append(os.path.join(root, filename))

#dbfilenames = dbfilenames[0:5]
source_group_name = "DeutscheBankAG"
source_firm_name = ""

table_switches = RTS27_Utilities.RTS27_TableSwitches("Y", "Y", "Y", "Y") #Table 1, Table 2, Table 4, and Table 6

fileId = 0
list_of_table2_records = []
list_of_table4_records = []
list_of_table6_records = []
fileId = 0
for filename in dbfilenames:
    #xml_file =  codecs.open(filename, 'r',encoding='utf-8')
    with open(filename, 'rb') as inputfile:
        header_string = ''
        fileId = fileId + 1
        print("Percentage Complete : " + str(round((float(fileId) / float(len(dbfilenames)) * 100), 2)) + "%")
        append = False
        appendComplete = False
        for line in inputfile:
            if '<Header>' in line:
                inputbuffer = line
                append = True
            elif '</Header>' in line:
                inputbuffer += line
                append = False
                header_string = inputbuffer
                inputbuffer = None
                del inputbuffer  # probably redundant..
                appendComplete = True
                break;
            elif append:
                inputbuffer += line

        for line in inputfile:
            if '<FinInstrument>' in line:
                finInstrument_string = line[line.find("<FinInstrument>"):line.find("</FinInstrument>") + len("</FinInstrument>")]
                processFinancialInstrument(finInstrument_string, header_string, rtsdb, rts_db_rd,fileId)

    #mydoc = minidom.parse(xml_file)

    #financialInstruments = mydoc.getElementsByTagName('FinInstrument')

    #trade_date = (mydoc.getElementsByTagName('DtTrdDay')[0].firstChild.data).encode('utf-8', errors='ignore')
    #venue =  (mydoc.getElementsByTagName('MktSgmt')[0].firstChild.data).encode('utf-8', errors='ignore')


