import fnmatch
import os
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime

import concurrent.futures

from Modules import FIRDS_Data_Module
from Modules_DB_Writers import FIRDS_DB_Writer_Module


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
    strikePriceString = find_between (buffer,"<StrkPric>","</StrkPric>")
    if strikePriceString!='':
        strkPricXML = ET.fromstring(strikePriceString)

        if (len(strkPricXML.findall('MntryVal/Amt')) > 0):
            strikePriceAmt = strkPricXML.findall('MntryVal/Amt')[0].text
            firds_data_rec.setStrikePriceAmt(strikePriceAmt)

        if (len(strkPricXML.findall('Pctg')) > 0):
            strikePricePctg = strkPricXML.findall('Pctg')[0].text
            firds_data_rec.setStrikePriceAmt(strikePricePctg)

        if (len(strkPricXML.findall('Yld')) > 0):
            strikePriceYld = strkPricXML.findall('Yld')[0].text
            firds_data_rec.setStrikePriceAmt(strikePriceYld)

        if (len(strkPricXML.findall('BsisPts')) > 0):
            strikePriceBasisPoints = strkPricXML.findall('BsisPts')[0].text
            firds_data_rec.setStrikePriceAmt(strikePriceBasisPoints)

    firds_data_rec.setOptionExerciseStyle(find_between (buffer,"<OptnExrcStyle>","</OptnExrcStyle>"))
    firds_data_rec.setRelevantCompetentAuthrity(find_between (buffer,"<RlvntCmptntAuthrty>","</RlvntCmptntAuthrty>"))
    firds_data_rec.setDebtTotalIssuedNominalAmount(find_between (buffer,"<TtlIssdNmnlAmt>","</TtlIssdNmnlAmt>"))
    debt_mat_date = find_between (buffer,"<MtrtyDt>","</MtrtyDt>")
    formatted_debt_mat_date = None
    if (debt_mat_date!='' and debt_mat_date!=' '):
        formatted_debt_mat_date = datetime.strptime(debt_mat_date, '%Y-%m-%d')
    firds_data_rec.setDebtMaturityDate(formatted_debt_mat_date)
    firds_data_rec.setDebtFixedRate(find_between (buffer,"<Fxd>","</Fxd>"))
    firds_data_rec.setFRDate(find_between (buffer,"<FrDt>","</FrDt>"))
    firds_data_rec.setFileName(filename)

    db_writer.Write_Data(firds_data_rec)

def process_zip_file(zipfilename):
    print ("Processing file " + zipfilename)
    firds_db_writer = FIRDS_DB_Writer_Module.FIRDS_DB_Writer()
    firdsxmlfilenames = []


    #print ("Processing file " + zipfilename + ", Percentage Complete : " + str(
    #    round((float(fileId) / float(len(firdszipfilenames)) * 100), 2)) + "%")
    # xml_file =  codecs.open(filename, 'r',encoding='utf-8')
    zipfileObj = zipfile.ZipFile(zipfilename)
    zipfileObj.extractall(os.path.dirname(zipfilename))
    for zip_content_name in zipfileObj.namelist():
        file_name_full_path = os.path.dirname(zipfilename)+"/"+ zip_content_name
        firdsxmlfilenames.append(file_name_full_path)
        print "Adding file " + file_name_full_path
    zipfileObj.close()

    #for root, dirnames, filenames in os.walk(firds_source_path):
    #    for xmlfilename in fnmatch.filter(filenames, '*.xml'):
    #        firdsxmlfilenames.append(os.path.join(root, xmlfilename))

    for xmlfilename in firdsxmlfilenames:

        esma_publish_date = xmlfilename.split("_")[1]
        with open(xmlfilename) as inputfile:
            append = False
            financial_instrument_count = 0
            inputbuffer = ""
            for chunk in read_in_chunks(inputfile):
                inputbuffer += chunk
                fin_Instr_start_loc = inputbuffer.find("<FinInstrm>")
                fin_Instr_end_loc = inputbuffer.find("</FinInstrm>")

                # Check if there is completion of an element here
                if (fin_Instr_end_loc != -1 and fin_Instr_start_loc != -1):
                    # Im assuming that if there is an end tag there will be a beginning tag also
                    finInstr_string = inputbuffer[fin_Instr_start_loc:fin_Instr_end_loc + len("</FinInstrm>")]
                    processFinancialInstrument(esma_publish_date, os.path.basename(xmlfilename), finInstr_string,
                                               firds_db_writer)

                    append = False
                    tempbuffer = inputbuffer[fin_Instr_end_loc + len("</FinInstrm>"):len(inputbuffer)]
                    inputbuffer = tempbuffer
                    financial_instrument_count += 1

        # zip up the file again
        os.remove(xmlfilename)



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
fileId = 0
# Create a pool of processes. By default, one is created for each CPU in your machine.
with concurrent.futures.ProcessPoolExecutor() as executor:

    firdszipfilenames = []
    for root, dirnames, filenames in os.walk(firds_source_path):
        for filename in fnmatch.filter(filenames, '*.zip'):
            firdszipfilenames.append(os.path.join(root, filename))

    firdszipfilenames.sort()

    # Process the list of files, but split the work across the process pool to use all CPUs!
    for zip_file  in zip(firdszipfilenames, executor.map(process_zip_file, firdszipfilenames)):
        print("zipfile {zip_file} was processed")

    #firdszipfilenames = firdszipfilenames[0:5]
