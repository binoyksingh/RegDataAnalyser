#!/usr/bin/python

import sys

class HistMktData:

    # Define list of attributes
    PRICE_DATE=""
    PRICE_TIMESTAMP=""
    ISIN=""
    ISIN_DESC=""
    CURRENCY_PAIR=""
    ISDA_ASSET_CLASS_ID=0
    ISDA_ASSET_CLASS_DESC=0.00
    SPOT_PRICE_OPEN_BID_QUOTE = 0.00
    SPOT_PRICE_CLOSE_BID_QUOTE = 0.00
    MKT_DATA_DETAIL = ""
    SOURCE_FILE_NAME =""
    SOURCE_DESC=""

    # def __init__(self) :
    #    print ("calling constructor - table 6")

    def setPriceDate(self, PRICE_DATE):
        self.PRICE_DATE = PRICE_DATE

    def setPriceTimestamp(self, PRICE_TIMESTAMP):
        self.PRICE_TIMESTAMP = PRICE_TIMESTAMP

    def setISIN(self, ISIN):
        self.ISIN = ISIN

    def setISINDesc(self, ISIN_DESC):
        self.ISIN_DESC = ISIN_DESC

    def setCurrencyPair(self, CURRENCY_PAIR):
        self.CURRENCY_PAIR = CURRENCY_PAIR

    def setISDAAssetClassID(self, ISDA_ASSET_CLASS_ID):
        self.ISDA_ASSET_CLASS_ID = ISDA_ASSET_CLASS_ID

    def setISDAAssetClassDesc(self, ISDA_ASSET_CLASS_DESC):
        self.ISDA_ASSET_CLASS_DESC = ISDA_ASSET_CLASS_DESC

    def setSpotPriceOpenBidQuote(self, SPOT_PRICE_OPEN_BID_QUOTE):
        self.SPOT_PRICE_OPEN_BID_QUOTE = SPOT_PRICE_OPEN_BID_QUOTE

    def setSpotPriceCloseBidQuote(self, SPOT_PRICE_CLOSE_BID_QUOTE):
        self.SPOT_PRICE_CLOSE_BID_QUOTE = SPOT_PRICE_CLOSE_BID_QUOTE

    def setMktDataDetail(self, MKT_DATA_DETAIL):
        self.MKT_DATA_DETAIL = MKT_DATA_DETAIL

    def setSourceFileName(self, SOURCE_FILE_NAME):
        self.SOURCE_FILE_NAME = SOURCE_FILE_NAME

    def setSourceDesc(self, SOURCE_DESC):
        self.SOURCE_DESC = SOURCE_DESC

    def getAttrArray(self):
        single_record_array = [self.PRICE_DATE,
                               self.PRICE_TIMESTAMP,
                               self.CURRENCY_PAIR,
                               self.ISIN,
                               self.ISIN_DESC,
                               self.ISDA_ASSET_CLASS_ID,
                               self.ISDA_ASSET_CLASS_DESC,
                               self.SPOT_PRICE_OPEN_BID_QUOTE,
                               self.SPOT_PRICE_CLOSE_BID_QUOTE,
                               self.MKT_DATA_DETAIL,
                               self.SOURCE_FILE_NAME,
                               self.SOURCE_DESC
                               ]
        return single_record_array