import csv
import glob

hsbc_source_path = "/Users/sarthakagarwal/PycharmProjects/MifidDataAnalyser/Source/Citi/UnZippedSource"
filenames = sorted(glob.glob(hsbc_source_path+'/SI_*.csv'))

#filenames = filenames[0:len(filenames)
filenames = filenames[0:100]

print ("Array Length is" + str(len(filenames)))
fileId = 0
for f in filenames:
   fileId = fileId + 1
   fileIdString = "CITI" + "_" + str(fileId)
   #print("Processing file" + f)

   # Table1 properties

   # Resetting all the variables
   table1_tradingday = ""
   table1_isin = ""
   table1_segement = ""
   table1_currency = ""
   table1_investmentFirm = ""
   table1_dateProduced = ""
   table1_dateofthetradingday = ""
   table1_venuename = ""
   table1_venue = ""
   table1_countryofthecompetentauthority = ""
   table1_marketsegementmic = ""
   table1_marketsegement = ""
   table1_outagesnature = ""
   table1_outagesnumber = ""
   table1_outagesaverageduration = ""

   # Table 2 properties
   table2_isin =""
   table2_instrumentname = ""
   table2_instrumentclassification = ""
   table2_currency = ""

   # Table 4 properties
   table4_simpleavgtransactionprice = 0
   table4_volumeweightedtransactionprice = 0
   table4_highestexecutedprice = 0
   table4_lowestexecutedprice = 0

   # Table 6 properties
   table6_numberofordersorrequest = ""
   table6_numberoftransactionsexecuted = ""
   table6_totalvalueoftransactionsexecuted = ""
   table6_numberoftransactionscancelledorwithdrawn = ""
   table6_numberoforderorrequestmodified = ""
   table6_mediantransactionsize = ""

   with open(f) as csvfile:
       readCSV = csv.reader(csvfile, delimiter=',')
       for row in readCSV:
           #print(row)
           #if ((row[6]=="Table 1:") and (row[7]is  not "Table 1 Start:") and (row[7] is not "Table 1 End:")) :
            if (len(row)>8):
                # Populate Table1 properties
                if (row[7] == "Investment Firm*"):
                    table1_investmentFirm = row[8]

                if (row[7] == "Date Produced*" ):
                    table1_dateProduced = row[8]

                if (row[7] == "Date of the Trading Day:" ):
                    table1_dateofthetradingday= row[8]

                if (row[7] == "Venue Name:"):
                    table1_venuename = row[8]

                if (row[7] == "Venue (ISO 10383 Market Identifier Code (MIC) or Legal Entity Identifier (LEI)):"):
                    table1_venue = row[8]

                if (row[7] == "Country of Competent Authority:"):
                    table1_countryofthecompetentauthority = row[8]

                if (row[7] == "Market Segment (ISO 10383 Market Segment MIC):"):
                    table1_marketsegementmic = row[8]

                if (row[7] == "Market Segment:"):
                    table1_marketsegement = row[8]

                if (row[7] == "Market Segment:"):
                    table1_marketsegement = row[8]

                # Populate Table2 properties
                if (row[7] == "Instrument Identifier (ISO 6166):" ):
                    table2_isin = row[8]

                if (row[7] == "Instrument Name:" ):
                    table2_instrumentname = row[8]

                if (row[7] == "Instrument Classification (ISO 10962 CFI Code):"):
                    table2_instrumentclassification = row[8]

                if (row[7] == "Currency (ISO 4217):"):
                    table2_currency = row[8]

                # Populate Table4 properties
                if (row[7] == "Simple average transaction price" ):
                    table4_simpleavgtransactionprice = row[8]

                if (row[7] == "Volume-weighted transaction price" ):
                    table4_volumeweightedtransactionprice = row[8]

                if (row[7] == "Highest executed price" ):
                    table4_highestexecutedprice = (row[8])

                if (row[7] == "Lowest executed price" ):
                    table4_lowestexecutedprice = row[8]

                # Populate Table 6 properties
                if (row[7] == "Number Of Order or Request for Quotes" ):
                    table6_numberofordersorrequest = row[8]
                if (row[7] == "Number Of Transactions Executed"):
                    table6_numberoftransactionsexecuted = row[8]
                if (row[7] == "Total Value of Transactions Executed"):
                    table6_totalvalueoftransactionsexecuted = row[8]
                if (row[7] == "Number Of Order or Request Cancelled/Withdrawn"):
                    table6_numberoftransactionscancelledorwithdrawn = row[8]
                if (row[7] == "Number Of Order or Request Modified"):
                    table6_numberoforderorrequestmodified = row[8]
                if (row[7] == "Median Transaction Size"):
                    table6_mediantransactionsize = row[8]


   # Printing output of Table 1
   print("CITI" + ";" + fileIdString + ";" + table2_isin + ";" + table1_dateofthetradingday + ";" + table1_investmentFirm + ";" + table1_dateProduced + ";" + table1_venuename + ";" + table1_venue + ";" + table1_countryofthecompetentauthority + ";" + table1_marketsegementmic + ";" + table1_marketsegement)

   # Printing output of Table 2
   print("CITI" + ";" + fileIdString + ";" + table2_isin + ";" + table1_dateofthetradingday + ";" + table1_venue + ";" + table2_instrumentname + ";" + table2_instrumentclassification + ";" + table2_currency)

   # Printing output of Table 4
   print("CITI" + ";" + fileIdString + ";" + table2_isin + ";" + table1_dateofthetradingday + ";" + table1_venue + ";" + str(table4_simpleavgtransactionprice) + ";" + str(table4_volumeweightedtransactionprice) + ";" + str(table4_highestexecutedprice) + ";" + str(table4_lowestexecutedprice))

   # Printing output of Table 6
   print("CITI" + ";" + fileIdString + ";" + table2_isin + ";" + table1_dateofthetradingday + ";" + table1_venue + ";" + table6_numberofordersorrequest + ";" + table6_numberoftransactionsexecuted + ";" + table6_totalvalueoftransactionsexecuted + ";" + table6_numberoftransactionscancelledorwithdrawn + ";" + table6_numberoforderorrequestmodified + ";" + table6_mediantransactionsize + ";"+ table2_currency)



