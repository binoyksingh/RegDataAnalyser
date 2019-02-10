#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import csv
import psycopg2
import csv
import glob
import os
import fnmatch
import datetime
import pandas as pd

from Modules import HistMktData_Module
from Modules_DB_Writers import HistMktData_DB_Writer_Module
from Modules_DB_Readers import HistMktData_DB_Reader_Module
from Utilities import HistDataLoader_Switches
import xml.etree.ElementTree as ET
from Modules import CurvePoint_Module
from Modules import Curve_Module
from decimal import Decimal


def getRateAtTenorFromCurve (ir_curve, tenor) :
    rate_at_tenor = 0
    for curve_point in ir_curve[0]:
        if (curve_point.TENOR == tenor):
            rate_at_tenor = curve_point.MIDPRICE
    return rate_at_tenor

def getLastRatesForDate( ir_hist_data, rate_date ):

    last_available_rates = []
    myIndex = pd.Index(ir_hist_data.keys()).sort_values(ascending=True)
    #myLoc = myIndex.get_loc(datetime.datetime.strptime('2018-01-03', '%Y-%m-%d'), method='ffill')
    myLoc = -1
    try :
        myLoc = myIndex.get_loc(rate_date, method='ffill')
    except KeyError:
        print "Count not find Rate for date" + str(rate_date)

    if (myLoc!=-1) :
        last_available_rates = ir_hist_data.get(myIndex[myLoc])

    return last_available_rates


def getFXForwardPriceFromFXSpot( fx_spot_price, tenor, domestic_ccy_ir_rate, foreign_ccy_ir_rate ):

    # Applying the FX Forward Formula Calculation as per below.
    # https://www.riskprep.com/all-tutorials/36-exam-22/59-calculating-forward-exchange-rates-covered-interest-parity
    try:
        tenor_days = getDaysInTenor(tenor)
        numerator = Decimal(1) + ((Decimal(domestic_ccy_ir_rate)/100) * (Decimal(tenor_days)/360))
        denominator = Decimal(1) + ((Decimal(foreign_ccy_ir_rate)/100) * (Decimal(tenor_days)/360))

        fx_forward_price = Decimal(fx_spot_price) * (numerator / denominator)

        return fx_forward_price
    except ValueError:
        print "getFXForwardPriceFromFXSpot ValueError"
        return 0


def getDaysInTenor( tenor ):
    tenor_days = 0
    if "M" in tenor:
        tenor_months = int(tenor.replace("M",""))
        tenor_days = int(tenor_months) * 30
    return tenor_days


histdata_loader_switches = HistDataLoader_Switches.HistDataLoader_Switches("Y","N","N","N") #FX, Rates,Equities, Bonds

histdata_db_writer = HistMktData_DB_Writer_Module.HistMktData_DB_Writer()
histdata_db_reader = HistMktData_DB_Reader_Module.HistMktData_DB_Reader()

fx_spot_rates_folder = "/Users/sarthakagarwal/PycharmProjects/HistRatesData/FXRatesMinuteData"
interest_rates_folder = "/Users/sarthakagarwal/PycharmProjects/HistRatesData/InterestRatesData"

histdata_filenames = []

if (histdata_loader_switches.LoadInterestRatesHistData == "Y"):

    for root, dirnames, filenames in os.walk(interest_rates_folder):
        for filename in fnmatch.filter(filenames, '*.csv'):
            histdata_filenames.append(os.path.join(root, filename))

    for filename in histdata_filenames:
        with open(filename) as csvfile:
            readCSV = csv.reader(csvfile, delimiter=',')
            next(readCSV)  # skip header
            for row in readCSV:

                date_str = str(row[1]).replace("\"","").replace("=","")
                date_obj = datetime.datetime.strptime(date_str, '%d/%m/%Y')

                # ------------------------------
                # Building HistMktData Object
                histdata_rec = HistMktData_Module.HistMktData()
                histdata_rec.setCurrencyPair(str(row[0]).replace("\"","").replace("=",""))
                histdata_rec.setISDAAssetClassID(7)
                histdata_rec.setISDAAssetClassDesc('INTEREST_RATE')
                histdata_rec.setPriceDate(str(date_obj))
                histdata_rec.setPriceTimestamp(str(date_obj))
                histdata_rec.setSourceDesc("Mecklai")
                histdata_rec.setSourceFileName(os.path.basename(filename))

                mkt_data_detail = "<InterestRateCurveData>"

                for col_num in range(2, len(row)-1):
                    mid_price = str(row[col_num]).replace("=","").replace("\"","")
                    mkt_data_detail += "<CurvePoint>"
                    mkt_data_detail += "<Tenor>" + str(col_num-1) + "M" +"</Tenor>"
                    mkt_data_detail += "<MidPrice>" + mid_price + "</MidPrice>"
                    mkt_data_detail += "</CurvePoint>"
                mkt_data_detail += "</InterestRateCurveData>"
                histdata_rec.setMktDataDetail(mkt_data_detail)

                print histdata_rec.getAttrArray()

                # Writing to HistMktData table
                histdata_db_writer.Write_Data(histdata_rec)

if (histdata_loader_switches.LoadFXHistData == "Y"):

    for root, dirnames, filenames in os.walk(fx_spot_rates_folder):
        for filename in fnmatch.filter(filenames, '*.csv'):
            histdata_filenames.append(os.path.join(root, filename))

    histdata_filenames = histdata_filenames[0:1]

    for filename in histdata_filenames:

        ccy_pair = filename.split("_")[3]
        first_currency = ccy_pair[0:3]
        last_currency = ccy_pair[3:6]

        sorted_ccy_list = [first_currency, last_currency]
        sorted_ccy_list.sort()
        sorted_ccy_pair = sorted_ccy_list[0] + sorted_ccy_list[1]

        # ------------------------------
        # Read the relevant Interest Rates
        first_ccy_ir_histdata = histdata_db_reader.getInterestRatesHistMktDataForCcy(first_currency)
        last_ccy_ir_histdata = histdata_db_reader.getInterestRatesHistMktDataForCcy(last_currency)

        with open(filename) as csvfile:
            readCSV = csv.reader(csvfile, delimiter=',')
            for row in readCSV:

                date_str = row[0]
                time_str = row[1]
                date_time_str = date_str + " " + time_str
                date_obj = datetime.datetime.strptime(date_str, '%Y.%m.%d')
                date_obj_key = date_obj.strftime('%Y-%m-%d')
                time_obj = datetime.datetime.strptime(time_str, '%H:%M')
                date_time_obj = datetime.datetime.strptime(date_time_str, '%Y.%m.%d %H:%M')
                spot_price_open_bid_quote = float(row[2])
                spot_price_close_bid_quote = float(row[5])

                domestic_ccy_rate = []
                foreign_ccy_rate = []

                try :
                    domestic_ccy_curve = getLastRatesForDate(last_ccy_ir_histdata, date_obj.date())
                    foreign_ccy_curve = getLastRatesForDate(first_ccy_ir_histdata, date_obj.date())

                except ValueError:
                    print "Problems getting rates"

                if ccy_pair != sorted_ccy_pair :
                        print "CCY Pair is not sorted. FX Rates would need to be inverted"
                        print sorted_ccy_pair
                        #spot_price_open_bid_quote = 1 / spot_price_open_bid_quote
                        #spot_price_close_bid_quote = 1 / spot_price_close_bid_quote

                # Build the FX Forwards Curve
                curve_rec = Curve_Module.Curve()
                curve_rec.setCurveType("FXFwdCurveData")
                if ((len(domestic_ccy_curve) !=0) and (len(foreign_ccy_curve)!=0)):
                    tenor_list = ["1M","2M", "3M","4M","5M","6M","7M","8M","9M","10M","11M","12M"]
                    curve_point_list = []

                    for tenor in tenor_list:
                        cp = CurvePoint_Module.Curve_Point()
                        dom_ccy_rate_at_tenor = getRateAtTenorFromCurve(domestic_ccy_curve, tenor)
                        for_ccy_rate_at_tenor = getRateAtTenorFromCurve(foreign_ccy_curve, tenor)

                        forward_price = getFXForwardPriceFromFXSpot(spot_price_open_bid_quote, tenor, dom_ccy_rate_at_tenor, for_ccy_rate_at_tenor )
                        if ccy_pair != sorted_ccy_pair:
                            print "CCY Pair is not sorted. FX Rates would need to be inverted"
                            print sorted_ccy_pair
                            forward_price = 1 / forward_price
                        cp.setTenor(tenor)
                        cp.setMidPrice(str(forward_price))
                        curve_rec.addCurvePoint(cp)
                        curve_point_list.append(cp)

                # ------------------------------
                # Building HistMktData Object
                histdata_rec = HistMktData_Module.HistMktData()
                histdata_rec.setCurrencyPair(sorted_ccy_pair)
                histdata_rec.setISDAAssetClassID(4)
                histdata_rec.setISDAAssetClassDesc('FOREIGN_EXCHANGE')
                histdata_rec.setPriceDate(str(date_obj))
                histdata_rec.setPriceTimestamp(str(date_time_obj))
                histdata_rec.setSpotPriceOpenBidQuote(spot_price_open_bid_quote)
                histdata_rec.setSpotPriceCloseBidQuote(spot_price_close_bid_quote)
                histdata_rec.setMktDataDetail(curve_rec.getMktDataDetailXML())

                print histdata_rec.getAttrArray()

                # Writing to HistMktData table
                histdata_db_writer.Write_Data(histdata_rec)

histdata_db_writer.__del__()