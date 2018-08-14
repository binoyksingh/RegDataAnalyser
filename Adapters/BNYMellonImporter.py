import fnmatch
import os
from datetime import datetime

from openpyxl import load_workbook

from Modules import RTS27_DB_Writer_Module, RTS27_Table_Records_Module

bnymellon_source_path = "/Users/sarthakagarwal/PycharmProjects/MifidDataAnalyser/Source/BNYMellon/Unzipped source"
#bnymellonfilenames = sorted(glob.iglob(bnymellon_source_path+'/**/*.xslx',recursive=True))
rtsdb = RTS27_DB_Writer_Module.RTS27_DB_Writer()

bnymellonfilenames = []
for root, dirnames, filenames in os.walk(bnymellon_source_path):
    for filename in fnmatch.filter(filenames, '*.xlsx'):
        bnymellonfilenames.append(os.path.join(root, filename))

#bnymellonfilenames = bnymellonfilenames[0:3]
source_firm_name = "BNYMellon"

print ("Array Length is" + str(len(bnymellon_source_path)))
fileId = 0
list_of_table2_records = []
list_of_table6_records = []
for filename in bnymellonfilenames:
    print("Processing file" + filename)
    wb = load_workbook(filename)
    wsheet = wb.worksheets[0] # reading the first sheet only

    CurrentFinancialInstrumentAtRow = 0
    PrevFinancialInstrumentAtRow = 0
    for rowcount in range(1, wsheet.max_row + 1):
        if ((wsheet.cell(row=rowcount , column=2)).value == "Type of Financial Instrument"):
            CurrentFinancialInstrumentAtRow = rowcount
            # Found an instrument, now to build the objects
            # Building Table 2 output
            rawdate = datetime.strptime((wsheet.cell(row=5 , column=2)).value, '%Y-%m-%d')
            formatted_date = datetime.strftime(rawdate, "%Y-%m-%d")
            table2_fileIdString = source_firm_name + "_" + str(rowcount)
            table2_instrumentname = str(str(wsheet.cell(row=rowcount+1 , column=2).value).decode('ascii', errors='ignore'))
            table2_isin = str(wsheet.cell(row=rowcount+2 , column=2).value)
            table2_instrumentclassification = str(wsheet.cell(row=rowcount+4 , column=2).value)
            table2_currency = str(wsheet.cell(row=rowcount+5 , column=2).value)
            table2_dateofthetradingday = formatted_date
            table2_venue = ""  # Since HSBC does not provide this Info...

            # Create structure
            table2_rec = RTS27_Table_Records_Module.RTS27_Table2(source_firm_name, filename, table2_fileIdString,
                                                                 table2_isin, table2_dateofthetradingday, table2_venue,
                                                                 table2_instrumentname, table2_instrumentclassification,
                                                                 table2_currency)
            # print table2_rec.getAttrArray()
            # Writing to Table 2
            #rtsdb.Write_to_Table2(table2_rec)

            # Building Table 6
            table6_rec_new = RTS27_Table_Records_Module.RTS27_Table6()
            table6_rec_new.SOURCE_COMPANY_NAME = source_firm_name
            table6_rec_new.FILENAME = filename
            table6_rec_new.TRADE_DATE = formatted_date
            table6_rec_new.FILE_ID = table2_fileIdString
            table6_rec_new.INSTRUMENT_NAME = table2_instrumentname
            table6_rec_new.ISIN = table2_isin
            table6_rec_new.CURRENCY = table2_currency

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

            # Writing output of Table 6
            # Create structure
            #table6_rec = RTS27_Table_Records_Module.setAttributes(source_firm_name, filename, table6_fileIdString,
            #                                                    table6_isin, table6_dateofthetradingday,
            #                                                     table6_instrumentname,
            #                                                     table6_numberofordersorrequestforquotes,
            #                                                     table6_numberoftransactionsexecuted,
            #                                                     table6_totalvalueoftransactionsexecuted,
            #                                                     table6_numberoftransactionscancelledorwithdrawn,
            #                                                     table6_numberoforderorrequestmodified,
            #                                                     table6_mediantransactionsize,
            #                                                     table6_mediansizeofallordersorrequestsforquote,
            #                                                     table6_numberofdesignatedmarketmakers, table6_currency)
            print (table6_rec_new.getAttrArray())
            rtsdb.Write_to_Table6(table6_rec_new)

            # RowJump
            # rowcount = rowcount + 36



