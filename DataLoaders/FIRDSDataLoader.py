import fnmatch
import os
from datetime import datetime
from openpyxl import load_workbook
from xml.dom import minidom
import xml.etree.ElementTree as ET
import ast, codecs
from Modules import RTS27_DB_Writer_Module, RTS27_Table_Records_Module, RTS27_Utilities
from Modules import RTS27_Prod_Class_DB_Reader_Module
from Modules import FIRDS_Data_Module
from Modules import FIRDS_DB_Writer_Module
import re
import csv
import zipfile


def processFinancialInstrument (publish_date, filename, buffer,db_writer) :
    #headerXML = ET.fromstring(buffer)

    firds_data_rec = FIRDS_Data_Module.FIRDS_Data()
    id = find_between (buffer,"<Id>","</Id>")
    firds_data_rec.setInstrumentIdentificationCode(find_between (buffer,"<Id>","</Id>"))
    firds_data_rec.setInstrumentFullName(find_between (buffer,"<FullNm>","</FullNm>"))
    firds_data_rec.setInstrumentClassification(find_between (buffer,"<ClssfctnTp>","</ClssfctnTp>"))
    firds_data_rec.setIssuerIdentifier(find_between (buffer,"<Issr>","</Issr>"))
    firds_data_rec.setTradingVenue(find_between (buffer,"<TradgVnRltdAttrbts><Id>","</Id>"))
    firds_data_rec.setInstrumentShortName(find_between (buffer,"<ShrtNm>","</ShrtNm>"))
    rawdate = find_between (buffer,"<TermntnDt>","</TermntnDt>")
    t_loc = rawdate.find("T")
    newdate = rawdate[:t_loc]
    formatted_date = None
    if (newdate!='' and newdate!=' '):
        formatted_date = datetime.strptime(newdate, '%Y-%m-%d')
    #formatted_date = datetime.strftime(rawdate, "%Y-%m-%d")
    firds_data_rec.setTerminationDate(formatted_date)
    firds_data_rec.setNotionalCurrency1(find_between (buffer,"<NtnlCcy>","</NtnlCcy>"))
    firds_data_rec.setExpiryDate(find_between (buffer,"<XpryDt>","</XpryDt>"))
    firds_data_rec.setDeliveryType(find_between (buffer,"<DlvryTp>","</DlvryTp>"))
    firds_data_rec.setFXType(find_between (buffer,"<DlvryTp>","</DlvryTp>"))
    firds_data_rec.setUnderlyingISIN(find_between (buffer,"<ISIN>","</ISIN>"))
    firds_data_rec.setOptionType(find_between (buffer,"<OptnTp>","</OptnTp>"))
    strikePriceAmt = find_between (buffer,"<Amt>","</Amt>")
    firds_data_rec.setStrikePriceAmt(strikePriceAmt)
    firds_data_rec.setOptionExerciseStyle(find_between (buffer,"<OptnExrcStyle>","</OptnExrcStyle>"))
    firds_data_rec.setRelevantCompetentAuthrity(find_between (buffer,"<RlvntCmptntAuthrty>","</RlvntCmptntAuthrty>"))

    firds_data_rec.setFRDate(find_between (buffer,"<FrDt>","</FrDt>"))
    firds_data_rec.setFileName(filename)

    db_writer.Write_Data(firds_data_rec)


def read_in_chunks(f, size=256):
    while True:
        chunk = f.read(size)
        if not chunk:
            break
        yield chunk

def find_between( s, first, last ):
    try:
        start = s.index( first ) + len( first )
        end = s.index( last, start )
        return s[start:end]
    except ValueError:
        return ""

firds_source_path = "/Users/sarthakagarwal/PycharmProjects/FIRDSData"

firds_db_writer = FIRDS_DB_Writer_Module.FIRDS_DB_Writer()

firdszipfilenames = []
for root, dirnames, filenames in os.walk(firds_source_path):
    for filename in fnmatch.filter(filenames, '*.zip'):
        firdszipfilenames.append(os.path.join(root, filename))

firdszipfilenames.sort()
fileId = 0
#firdszipfilenames = firdszipfilenames[0:5]
for zipfilename in firdszipfilenames:
    fileId = fileId + 1
    print ("Processing file " + zipfilename + ", Percentage Complete : " + str(round((float(fileId) / float(len(firdszipfilenames)) * 100), 2)) + "%")
    #xml_file =  codecs.open(filename, 'r',encoding='utf-8')
    zipfileObj = zipfile.ZipFile(zipfilename)
    zipfileObj.extractall(os.path.dirname(zipfilename))
    zipfileObj.close()

    firdsxmlfilenames = []
    for root, dirnames, filenames in os.walk(firds_source_path):
        for xmlfilename in fnmatch.filter(filenames, '*.xml'):
            firdsxmlfilenames.append(os.path.join(root, xmlfilename))

    for xmlfilename in firdsxmlfilenames:

        esma_publish_date=xmlfilename.split("_")[1]
        with open(xmlfilename) as inputfile:
            append = False
            financial_instrument_count = 0
            inputbuffer=""
            for chunk in read_in_chunks(inputfile):
                    inputbuffer += chunk
                    fin_Instr_start_loc = inputbuffer.find("<FinInstrm>")
                    fin_Instr_end_loc = inputbuffer.find("</FinInstrm>")

                    # Check if there is completion of an element here
                    if (fin_Instr_end_loc != -1 and fin_Instr_start_loc!=-1):
                        #Im assuming that if there is an end tag there will be a beginning tag also
                        finInstr_string = inputbuffer[fin_Instr_start_loc:fin_Instr_end_loc + len("</FinInstrm>")]
                        processFinancialInstrument(esma_publish_date,os.path.basename(filename),finInstr_string, firds_db_writer )

                        append = False
                        tempbuffer = inputbuffer[fin_Instr_end_loc + len("</FinInstrm>"):len(inputbuffer)]
                        inputbuffer = tempbuffer
                        financial_instrument_count +=1

        # zip up the file again
        os.remove(xmlfilename)