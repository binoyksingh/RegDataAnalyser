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
import re
import csv


def processFinancialInstrument (publish_date, filename, buffer) :
    print buffer
    #headerXML = ET.fromstring(buffer)

    firds_data_rec = FIRDS_Data_Module.FIRDS_Data()
    id = find_between (buffer,"<Id>","</Id>")
    firds_data_rec.setInstrumentIdentificationCode(find_between (buffer,"<Id>","</Id>"))
    firds_data_rec.setInstrumentFullName(find_between (buffer,"<FullNm>","</FullNm>"))
    firds_data_rec.setInstrumentClassification(find_between (buffer,"<ClssfctnTp>","</ClssfctnTp>"))
    firds_data_rec.setIssuerIdentifier(find_between (buffer,"<Issr>","</Issr>"))
    firds_data_rec.setTradingVenue(find_between (buffer,"<Id>","</Id>"))
    firds_data_rec.setInstrumentShortName(find_between (buffer,"<ShrtNm>","</ShrtNm>"))
    firds_data_rec.setTerminationDate(find_between (buffer,"<TermntnDt>","</TermntnDt>"))
    firds_data_rec.setNotionalCurrency1(find_between (buffer,"<NtnlCcy>","</NtnlCcy>"))
    firds_data_rec.setExpiryDate(find_between (buffer,"<XpryDt>","</XpryDt>"))
    firds_data_rec.setDeliveryType(find_between (buffer,"<DlvryTp>","</DlvryTp>"))
    firds_data_rec.setFXType(find_between (buffer,"<DlvryTp>","</DlvryTp>"))
    firds_data_rec.setUnderlyingISIN(find_between (buffer,"<ISIN>","</ISIN>"))
    firds_data_rec.setOptionType(find_between (buffer,"<OptnTp>","</OptnTp>"))
    firds_data_rec.setStrikePriceAmt(find_between (buffer,"<Amt>","</Amt>"))
    firds_data_rec.setOptionExerciseStyle(find_between (buffer,"<OptnExrcStyle>","</OptnExrcStyle>"))
    firds_data_rec.setRelevantCompetentAuthrity(find_between (buffer,"<RlvntCmptntAuthrty>","</RlvntCmptntAuthrty>"))
    firds_data_rec.setFRDate(find_between (buffer,"<FrDt>","</FrDt>"))
    firds_data_rec.setFileName(filename)




    print firds_data_rec.getAttrArray()



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

firdsfilenames = []
for root, dirnames, filenames in os.walk(firds_source_path):
    for filename in fnmatch.filter(filenames, '*.xml'):
        firdsfilenames.append(os.path.join(root, filename))

for filename in firdsfilenames:
    #xml_file =  codecs.open(filename, 'r',encoding='utf-8')
    esma_publish_date=filename.split("_")[1]
    with open(filename) as inputfile:

        append = False
        financial_instrument_count = 0
        inputbuffer=""
        for chunk in read_in_chunks(inputfile):
                financial_instrument_count += 1
                inputbuffer += chunk
                fin_Instr_start_loc = inputbuffer.find("<FinInstrm>")
                fin_Instr_end_loc = inputbuffer.find("</FinInstrm>")

                # Check if there is completion of an element here
                if (fin_Instr_end_loc != -1 and fin_Instr_start_loc!=-1):
                    #Im assuming that if there is an end tag there will be a beginning tag also
                    finInstr_string = inputbuffer[fin_Instr_start_loc:fin_Instr_end_loc + len("</FinInstrm>")]
                    processFinancialInstrument(esma_publish_date,os.path.basename(filename),finInstr_string)

                    append = False
                    tempbuffer = inputbuffer[fin_Instr_end_loc + len("</FinInstrm>"):len(inputbuffer)]
                    inputbuffer = tempbuffer
                    print "tempbuffer is" + tempbuffer
                    #financial_instrument_count +=1
                    print "financial Instrument count" + str(financial_instrument_count)


                #elif append:
                #    inputbuffer += chunk

                # Check if there is a new element starting here as well (because)
                #if (fin_Instr_start_loc != -1 and append == False):
                #        inputbuffer = chunk[fin_Instr_start_loc:len(chunk)]
                #        append = True
