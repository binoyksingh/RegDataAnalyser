#!/usr/bin/python

import psycopg2, sys
import ConfigParser
from collections import defaultdict
import xml.etree.ElementTree as ET
from Modules import CurvePoint_Module
import datetime

class HistMktData_DB_Reader:

    connection = psycopg2._connect
    config = ConfigParser.ConfigParser()

    def __init__(self):
        print ('HistMktData_DB_Reader:INIT:Calling Constructor')
        self.config.read("../Config/DatabaseConfig.txt")

        DB_HOSTNAME_POSTGRES = self.config.get("DATABASE_DETAILS", "DATABASE_HOST_POSTGRES")
        DB_USER_POSTGRES = self.config.get("DATABASE_DETAILS", "DATABASE_USER_POSTGRES")
        DB_PASSWORD_POSTGRES = self.config.get("DATABASE_DETAILS", "DATABASE_PASSWORD_POSTGRES")
        DB_NAME_POSTGRES = self.config.get("DATABASE_DETAILS", "DATABASE_NAME_POSTGRES")

        self.connection = psycopg2.connect(host=DB_HOSTNAME_POSTGRES, user=DB_USER_POSTGRES,
                                           password=DB_PASSWORD_POSTGRES, dbname=DB_NAME_POSTGRES)

        self.cursor = self.connection.cursor()

        print ('RTS27_DB_WRITER:INIT:Connection Success')

    def __del__(self):
        print ('RTS27_DB_WRITER:INIT:Calling destructor')
        self.connection.close()

    # Initialise the CFI Asset Class Map
    def getInterestRatesHistMktDataForCcy(self, ccy):

        print "calling : getInterestRatesHistMktDataForCcy WITH : " + ccy
        select_InterestRateHistMktData_string = "SELECT price_date, mkt_data_detail, currency_pair" \
                                       " from hist_market_data where currency_pair = %s  "

        self.cursor.execute(select_InterestRateHistMktData_string, (ccy,))

        myresult =  self.cursor.fetchall()
        print myresult

        # Build the FX Forwards Curve
        histdata_map = {}

        for x in myresult:

            mktDataDetailXMLStr = x[1]
            curve_point_list = []

            mktDataDetailXML = ET.fromstring(mktDataDetailXMLStr)
            curvePointsXML = mktDataDetailXML.findall('CurvePoint')

            for curve_point in curvePointsXML:
                cp = CurvePoint_Module.Curve_Point()

                for child in curve_point:
                    if (child.tag=="Tenor"):
                        cp.setTenor(child.text)

                    if (child.tag=="MidPrice"):
                        cp.setMidPrice(child.text)

                curve_point_list.append(cp)

            histdata_map[x[0]] = [curve_point_list]

        return histdata_map