import fnmatch
import os
from datetime import datetime
from openpyxl import load_workbook
from Modules import RTS27_DB_Writer_Module, RTS27_Table_Records_Module, RTS27_Utilities
from Modules import RTS27_Prod_Class_DB_Reader_Module
from Modules import RTS27_LEI_Company_Map_Module

#
# PRIMARY Key to Join Tables 2,4,6 :
# Company Name
# ISIN
# Trade Date
# CCY
#

bnymellon_source_path = "/Users/sarthakagarwal/PycharmProjects/MifidDataAnalyser/Source/BNYMellon/Unzipped source"
#bnymellonfilenames = sorted(glob.iglob(bnymellon_source_path+'/**/*.xslx',recursive=True))
rtsdb = RTS27_DB_Writer_Module.RTS27_DB_Writer()

bnymellonfilenames = []
for root, dirnames, filenames in os.walk(bnymellon_source_path):
    for filename in fnmatch.filter(filenames, '*.xlsx'):
        bnymellonfilenames.append(os.path.join(root, filename))

bnymellonfilenames = bnymellonfilenames[0:3]
source_firm_group_name = "BNYMellon"
source_company_name=""
rts_db_rd = RTS27_Prod_Class_DB_Reader_Module.RTS27_Prod_Class_DB_Reader()

table_switches = RTS27_Utilities.RTS27_TableSwitches("N","N","N","N") #Table 1, Table 2, Table 3, and Table 4

fileId = 0
list_of_table2_records = []
list_of_table6_records = []
for filename in bnymellonfilenames:
    print("Processing file" + os.path.basename(filename))
    wb = load_workbook(filename)
    wsheet = wb.worksheets[0] # reading the first sheet only

    CurrentFinancialInstrumentAtRow = 0
    PrevFinancialInstrumentAtRow = 0
    formatted_date = ""
    company_code = ""

    for rowcount in range(1, wsheet.max_row + 1):
        if ((wsheet.cell(row=rowcount , column=2)).value == "Type of Execution Venue"):
            # Building Table 1 object
            table1_rec = RTS27_Table_Records_Module.RTS27_Table1()
            table1_rec.setSourceCompanyCode(str(str(wsheet.cell(row=rowcount+1 , column=2).value).decode('ascii', errors='ignore')))
            company_code = str(wsheet.cell(row=rowcount+1 , column=2).value)
            lei_comp_obj = RTS27_LEI_Company_Map_Module.RTS27_LEI_Company_Map()
            source_company_name = lei_comp_obj.getMap()[company_code]
            table1_rec.setSourceCompanyGroupName(source_firm_group_name)
            table1_rec.setSourceCompanyName(source_company_name)
            rawdate = datetime.strptime((wsheet.cell(row=5 , column=2)).value, '%Y-%m-%d')
            formatted_date = datetime.strftime(rawdate, "%Y-%m-%d")
            table1_rec.setTradeDate(formatted_date)
            table1_rec.setCountryOfCompetentAuthority((wsheet.cell(row=3 , column=2)).value)
            table1_rec.setMarketSegmentID((wsheet.cell(row=4, column=2)).value)
            table1_rec.setFileName(os.path.basename(filename))
            table1_rec.setFailedTransactionsNumber((wsheet.cell(row=8, column=2)).value)

        if ((wsheet.cell(row=rowcount , column=2)).value == "Type of Financial Instrument"):

            CurrentFinancialInstrumentAtRow = rowcount
            # Found an instrument, now to build the objects
            # Building Table 2 object
            table2_rec = RTS27_Table_Records_Module.RTS27_Table2(rts_db_rd.getCfi_assetclass_map(), rts_db_rd.getCfi_char_map())
            table2_rec.setTradeDate(formatted_date)
            table2_rec.setInstrumentName(str(str(wsheet.cell(row=rowcount+1 , column=2).value).decode('ascii', errors='ignore')))
            table2_rec.setISIN(str(wsheet.cell(row=rowcount+2 , column=2).value))
            table2_rec.setInstrumentClassification(str(wsheet.cell(row=rowcount+4 , column=2).value))
            table2_rec.setCurrency(str(wsheet.cell(row=rowcount+5 , column=2).value))

            table2_rec.setSourceCompanyName(source_company_name)
            table2_rec.setFileName(os.path.basename(filename))
            table2_rec.setTradeDate(formatted_date)

            table2_rec.setFileId(company_code +"_"+ formatted_date +"_"+ table2_rec.ISIN + "_" + table2_rec.CURRENCY)

            # -----------------------------------------------------------
            # Building Table 4
            table4_rec_new = RTS27_Table_Records_Module.RTS27_Table4()
            table4_rec_new.SOURCE_COMPANY_NAME = source_company_name
            table4_rec_new.FILENAME = os.path.basename(filename)
            table4_rec_new.TRADE_DATE = formatted_date
            table4_rec_new.FILE_ID = table2_rec.FILE_ID
            table4_rec_new.INSTRUMENT_NAME = table2_rec.INSTRUMENT_NAME
            table4_rec_new.ISIN = table2_rec.ISIN
            table4_rec_new.CURRENCY = table2_rec.CURRENCY

            if (wsheet.cell(row=rowcount + 24, column=2).value is not None):
                table4_rec_new.SIMPLE_AVERAGE_TRANSACTION_PRICE = str(wsheet.cell(row=rowcount + 24, column=2).value)
            if (wsheet.cell(row=rowcount + 25, column=2).value is not None):
                table4_rec_new.VOLUME_WEIGHTED_TRANSACTION_PRICE = str(wsheet.cell(row=rowcount + 25, column=2).value)
            if (wsheet.cell(row=rowcount + 26, column=2).value is not None):
                table4_rec_new.HIGHEST_EXECUTED_PRICE = str(wsheet.cell(row=rowcount + 26, column=2).value)
            if (wsheet.cell(row=rowcount + 27, column=2).value is not None):
                table4_rec_new.LOWEST_EXECUTED_PRICE = str(wsheet.cell(row=rowcount + 27, column=2).value)

            # -----------------------------------------------------------
            # Building Table 6
            table6_rec_new = RTS27_Table_Records_Module.RTS27_Table6()
            table6_rec_new.SOURCE_COMPANY_NAME = source_company_name
            table6_rec_new.FILENAME = os.path.basename(filename)
            table6_rec_new.TRADE_DATE = formatted_date
            table6_rec_new.FILE_ID = table2_rec.FILE_ID
            table6_rec_new.INSTRUMENT_NAME = table2_rec.INSTRUMENT_NAME
            table6_rec_new.ISIN = table2_rec.ISIN
            table6_rec_new.CURRENCY = table2_rec.CURRENCY

            if (wsheet.cell(row=rowcount+30 , column=2).value is not None ) :
                table6_rec_new.NUMBER_OF_ORDER_OR_REQUEST_FOR_QUOTE = int(wsheet.cell(row=rowcount+30 , column=2).value)
            if (wsheet.cell(row=rowcount+31 , column=2).value is not None ) :
                table6_rec_new.NUMBER_OF_TRANSACTIONS_EXECUTED = int(wsheet.cell(row=rowcount+31 , column=2).value)
            if (wsheet.cell(row=rowcount+32 , column=2).value is not None ) :
                table6_rec_new.TOTAL_VALUE_OF_TRANSACTIONS_EXECUTED = str(wsheet.cell(row=rowcount+32 , column=2).value)

            if (wsheet.cell(row=rowcount+33 , column=2).value is not None ) :
                table6_rec_new.NUMBER_OF_ORDERS_OR_REQUEST_CANCELLED_OR_WITHDRAWN = int(wsheet.cell(row=rowcount+33 , column=2).value)

            if (wsheet.cell(row=rowcount+34 , column=2).value is not None ) :
                table6_rec_new.NUMBER_OF_ORDERS_OR_REQUEST_MODIFIED = (wsheet.cell(row=rowcount+34 , column=2).value)

            if (wsheet.cell(row=rowcount+35 , column=2).value is not None ) :
                table6_rec_new.MEDIAN_TRANSACTION_SIZE =  str(wsheet.cell(row=rowcount+35 , column=2).value)

            if (wsheet.cell(row=rowcount+36 , column=2).value is not None ) :
                table6_rec_new.MEDIAN_SIZE_OF_ALL_ORDERS_OR_REQUESTS_FOR_QUOTE = str(wsheet.cell(row=rowcount+36 , column=2).value)

            if (wsheet.cell(row=rowcount+37 , column=2).value is not None ) :
                table6_rec_new.NUMBER_OF_DESIGNATED_MARKET_MAKER = str(wsheet.cell(row=rowcount+37 , column=2).value)

            table1_rec.FILE_ID = table2_rec.FILE_ID

            if (table_switches.PROCESS_TABLE_1 == "Y"):
                # Writing to Table 1 to Database
                rtsdb.Write_to_Table1(table1_rec)

            if (table_switches.PROCESS_TABLE_2 == "Y") :
                # Writing to Table 2 to Database
                rtsdb.Write_to_Table2(table2_rec)

            if (table_switches.PROCESS_TABLE_4 == "Y") :
                # Writing to Table 4 to Database
                rtsdb.Write_to_Table4(table4_rec_new)

            if (table_switches.PROCESS_TABLE_6 == "Y") :
                # Writing to Table 6 to Database
                rtsdb.Write_to_Table6(table6_rec_new)