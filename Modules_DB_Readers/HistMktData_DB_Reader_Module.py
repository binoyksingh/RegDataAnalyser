#!/usr/bin/python

import psycopg2, sys
import ConfigParser
from collections import defaultdict
import xml.etree.ElementTree as ET
from Modules import CurvePoint_Module
from Modules import Curve_Module
import datetime
import os

class HistMktData_DB_Reader:

    connection = psycopg2._connect
    config = ConfigParser.ConfigParser()

    def __init__(self):
        print ('HistMktData_DB_Reader:INIT:Calling Constructor')
        #self.config.read("../Config/DatabaseConfig.txt")
        self.config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), '../Config', 'DatabaseConfig.txt'))

        DB_HOSTNAME_POSTGRES = self.config.get("DATABASE_DETAILS", "DATABASE_HOST_POSTGRES")
        DB_USER_POSTGRES = self.config.get("DATABASE_DETAILS", "DATABASE_USER_POSTGRES")
        DB_PASSWORD_POSTGRES = self.config.get("DATABASE_DETAILS", "DATABASE_PASSWORD_POSTGRES")
        DB_NAME_POSTGRES = self.config.get("DATABASE_DETAILS", "DATABASE_NAME_POSTGRES")

        self.connection = psycopg2.connect(host=DB_HOSTNAME_POSTGRES, user=DB_USER_POSTGRES,
                                           password=DB_PASSWORD_POSTGRES, dbname=DB_NAME_POSTGRES)

        self.cursor = self.connection.cursor()

        print ('HistMktData_DB_Reader:INIT:Connection Success')

    def __del__(self):
        print ('HistMktData_DB_Reader:INIT:Calling destructor')
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

    # get FX Forward rate closest to the trade execution time
    def getFXFwdHistMktDataForCcy(self, currency_pair, trade_execution_timestamp, tenor_day):

        select_FXHistMktDataString = "select price_ts, currency_pair, fx_spot_price_open_bid_quote, mkt_Data_detail" \
                                     " from hist_market_data where price_ts in ( " \
                                     " select max(price_ts) from hist_market_data where currency_pair=%s and " \
                                      " price_ts < %s) and currency_pair=%s "

        self.cursor.execute(select_FXHistMktDataString, (currency_pair,trade_execution_timestamp,currency_pair))

        myresult =  self.cursor.fetchall()
        curve = Curve_Module.Curve()
        fx_mid_price_for_ccy_tenor = 0

        for x in myresult:

            mktDataDetailXMLStr = x[3]
            spot_price = x[2]

            # Put all this stuff into the curve class
            cp_spot = CurvePoint_Module.Curve_Point()
            cp_spot.setTenor("0M")
            cp_spot.setMidPrice(spot_price)

            # print mktDataDetailXMLStr
            curve.addCurvePoint(cp_spot)

            mktDataDetailXML = ET.fromstring(mktDataDetailXMLStr)
            curvePointsXML = mktDataDetailXML.findall('CurvePoint')

            for curve_point in curvePointsXML:
                cp = CurvePoint_Module.Curve_Point()

                for child in curve_point:
                    if (child.tag == "Tenor"):
                        cp.setTenor(child.text)

                    if (child.tag == "MidPrice"):
                        cp.setMidPrice(child.text)

                curve.addCurvePoint(cp)

            fx_mid_price_for_ccy_tenor = curve.getPriceForTenor(tenor_day)

        return fx_mid_price_for_ccy_tenor

    # get FX Spot rate closest to the trade execution time
    def getFXSpotHistMktDataForCcyPair(self, currency_pair, trade_execution_timestamp):

        select_FXHistMktDataString = "select price_ts, currency_pair, fx_spot_price_open_bid_quote, mkt_Data_detail" \
                                     " from hist_market_data where price_ts in ( " \
                                     " select max(price_ts) from hist_market_data where currency_pair=%s and " \
                                     " price_ts < %s) and currency_pair=%s "

        self.cursor.execute(select_FXHistMktDataString, (currency_pair, trade_execution_timestamp, currency_pair))

        myresult = self.cursor.fetchall()
        curve = Curve_Module.Curve()
        fx_mid_price_for_ccy_tenor = 0

        for x in myresult:

            fx_spot_price_for_ccy = x[2]

        return fx_spot_price_for_ccy