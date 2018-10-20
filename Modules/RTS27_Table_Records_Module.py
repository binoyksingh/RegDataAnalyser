#!/usr/bin/python

import pymysql, sys
import re
from datetime import datetime
from decimal import Decimal
from  ProductClassification import AssetClassModule

class RTS27_Table2:

    SOURCE_COMPANY_NAME = ""
    FILENAME = ""
    FILE_ID = ""
    ISIN = ""
    TRADE_DATE = ""
    VENUE = ""
    INSTRUMENT_NAME = ""
    INSTRUMENT_CLASSIFICATION = ""
    CURRENCY = ""

    # currency and value date attributes
    CCY_PAIR = ""
    CCY1 = ""
    CCY2 = ""
    VALUE_DATE = ""
    TENOR = ""

    # CFI Codes
    CFI_ATTR_1_DESC = ""
    CFI_ATTR_2_DESC = ""
    CFI_ATTR_3_DESC = ""
    CFI_ATTR_4_DESC = ""
    CFI_ATTR_5_DESC = ""
    CFI_ATTR_6_DESC = ""

    # ISDA Codes
    ISDA_ASSET_CLASS_ID = AssetClassModule.AssetClass.UNCLASSIFIED     ### 1 - stands for Unclassified
    ISDA_ASSET_CLASS_DESC = AssetClassModule.AssetClass.getDesc(AssetClassModule.AssetClass.UNCLASSIFIED)   ## Default should be UNCLASSIFIED

    ccy_list_static = "(AFN|EUR|DZD|USD|AOA|XCD|ARS|AMD|AWG|AUD|AZN|BSD|BHD|BDT|BBD|BYN|BZD|XOF|BMD|INR|BTN|BOB|BOV|BAM|BWP|NOK|BRL|BND|BGN|BIF|CVE|KHR|XAF|CAD|KYD|CLP|CLF|CNY|COP|COU|KMF|CDF|NZD|CRC|HRK|CUP|CUC|ANG|CZK|DKK|DJF|DOP|EGP|SVC|ERN|ETB|FKP|FJD|XPF|GMD|GEL|GHS|GIP|GTQ|GBP|GNF|GYD|HTG|HNL|HKD|HUF|ISK|IDR|XDR|IRR|IQD|ILS|JMD|JPY|JOD|KZT|KES|KPW|KRW|KWD|KGS|LAK|LBP|LSL|ZAR|LRD|LYD|CHF|MOP|MKD|MGA|MWK|MYR|MVR|MRU|MUR|XUA|MXN|MXV|MDL|MNT|MAD|MZN|MMK|NAD|NPR|NIO|NGN|OMR|PKR|PAB|PGK|PYG|PEN|PHP|PLN|QAR|RON|RUB|RWF|SHP|WST|STN|SAR|RSD|SCR|SLL|SGD|XSU|SBD|SOS|SSP|LKR|SDG|SRD|SZL|SEK|CHE|CHW|SYP|TWD|TJS|TZS|THB|TOP|TTD|TND|TRY|TMT|UGX|UAH|AED|USN|UYU|UYI|UYW|UZS|VUV|VES|VND|YER|ZMW|ZWL|XBA|XBB|XBC|XBD|XTS|XAU|XPD|XPT|XAG)"

    def __init__(self, cfi_assetclass_map, cfi_char_map ) :
        self.cfi_assetclass_map = cfi_assetclass_map
        self.cfi_char_map = cfi_char_map

    def setAttributes(self, SOURCE_COMPANY_NAME, FILENAME,FILE_ID , ISIN, TRADE_DATE, VENUE, INSTRUMENT_NAME, INSTRUMENT_CLASSIFICATION, CURRENCY):
        self.SOURCE_COMPANY_NAME = SOURCE_COMPANY_NAME
        self.FILENAME = FILENAME
        self.FILE_ID = FILE_ID
        self.ISIN = ISIN
        self.TRADE_DATE = TRADE_DATE
        self.VENUE = VENUE
        self.INSTRUMENT_NAME = INSTRUMENT_NAME
        self.INSTRUMENT_CLASSIFICATION = INSTRUMENT_CLASSIFICATION
        self.CURRENCY = CURRENCY

    def setSourceCompanyName(self, SOURCE_COMPANY_NAME):
            self.SOURCE_COMPANY_NAME = SOURCE_COMPANY_NAME

    def setISIN(self, ISIN):
            self.ISIN = ISIN

    def setFileName(self, FILENAME):
            self.FILENAME = FILENAME

    def setFileId(self, FILE_ID):
        self.FILE_ID = FILE_ID

    def setTradeDate(self, TRADE_DATE):
        self.TRADE_DATE = TRADE_DATE

    def setVenue(self, VENUE):
        self.VENUE = VENUE

    def setInstrumentName(self, INSTRUMENT_NAME):
        self.INSTRUMENT_NAME = INSTRUMENT_NAME[:255]

        if (self.INSTRUMENT_NAME!=""):
            regex1 = r" " + self.ccy_list_static + self.ccy_list_static + " "
            regex2 = r" " + self.ccy_list_static + " " + self.ccy_list_static + " "
            regex3 = r" [0-9]{8}$"

            match1 = re.search(regex1, self.INSTRUMENT_NAME.upper())
            if (match1 is not None):
                self.CCY1 = self.INSTRUMENT_NAME[match1.start():match1.start()+4]
                self.CCY2 = self.INSTRUMENT_NAME[match1.start()+4:match1.start()+7]

            match2 = re.search(regex2, self.INSTRUMENT_NAME.upper())
            if (match2 is not None):
                self.CCY1 = self.INSTRUMENT_NAME[match2.start():match2.start() + 4]
                self.CCY2 = self.INSTRUMENT_NAME[match2.start() + 5:match2.start() + 8]

            self.CCY_PAIR = min(self.CCY1,self.CCY2) + max(self.CCY1,self.CCY2)

            match3 = re.search(regex3, self.INSTRUMENT_NAME.upper())
            self.VALUE_DATE = None
            if (match3 is not None):
                print "value date found in instrument name is " + self.INSTRUMENT_NAME[match3.start():match3.end()] + ",for insrtrument" + self.INSTRUMENT_NAME
                val_date = self.INSTRUMENT_NAME[match3.start():match3.end()]

                try:
                    rawdate = datetime.strptime(val_date.strip(), '%Y%m%d')
                    self.VALUE_DATE = datetime.strftime(rawdate, "%Y-%m-%d")
                    self.TENOR = self.getTenor(self.TRADE_DATE, self.VALUE_DATE)

                except ValueError:
                    print "value date found in instrument name is " + self.INSTRUMENT_NAME

            #print ("CCY1:" + self.CCY1 + ", CCY2:" + self.CCY2 + ", CCYPAIR:" + self.CCY_PAIR + ", VALUE DATE " +
            #       self.VALUE_DATE + ", TRADE DATE:"+ self.TRADE_DATE +", TENOR:" + self.TENOR)


    def setInstrumentClassification(self, INSTRUMENT_CLASSIFICATION):
        if ((self.cfi_assetclass_map!=None) and(self.cfi_char_map!=None)):
            self.INSTRUMENT_CLASSIFICATION = INSTRUMENT_CLASSIFICATION.upper()

            self.ISDA_ASSET_CLASS_ID = self.getAssetClassEnum(self.cfi_assetclass_map)
            self.ISDA_ASSET_CLASS_DESC = AssetClassModule.AssetClass.getDesc(self.ISDA_ASSET_CLASS_ID)

            cfi_group = (self.INSTRUMENT_CLASSIFICATION[:2]).upper()
            if (self.INSTRUMENT_CLASSIFICATION!="" and self.INSTRUMENT_CLASSIFICATION!=' '):
                asset_class_desc = self.cfi_assetclass_map.get(cfi_group)
                if (asset_class_desc!=None):
                    self.setCFIAttr(self.cfi_assetclass_map[cfi_group][0], "ATTR_1" )
                    self.setCFIAttr(self.cfi_assetclass_map[cfi_group][1], "ATTR_2" )
                else :
                    self.setCFIAttr("UNCLASSIFIED", "ATTR_1")
                    self.setCFIAttr("UNCLASSIFIED", "ATTR_2")

                self.setCFIAttr(self.getCFIAttr_3_Desc(self.cfi_char_map),"ATTR_3" )
                self.setCFIAttr(self.getCFIAttr_4_Desc(self.cfi_char_map),"ATTR_4" )
                self.setCFIAttr(self.getCFIAttr_5_Desc(self.cfi_char_map), "ATTR_5" )
                self.setCFIAttr(self.getCFIAttr_6_Desc(self.cfi_char_map), "ATTR_6" )

    def setCurrency(self,  CURRENCY):
        self.CURRENCY = CURRENCY

    def getTenor(self,  TRADE_DATE, VALUE_DATE):
        tenor = ""
        if (TRADE_DATE!="" and VALUE_DATE!=""):
            days_diff = datetime.strptime(VALUE_DATE, "%Y-%m-%d") - datetime.strptime(TRADE_DATE, "%Y-%m-%d")

            if ((days_diff.days >= 0) and (days_diff.days < 2)):
                tenor = "O/N"
            elif ((days_diff.days >= 2) and (days_diff.days < 8)):
                tenor = "1W"
            elif ((days_diff.days >= 8) and (days_diff.days < 15)):
                tenor = "2W"
            elif ((days_diff.days >= 15) and (days_diff.days < 32)):
                tenor = "1M"
            elif ((days_diff.days >= 32) and (days_diff.days < 64)):
                tenor = "2M"
            elif ((days_diff.days >= 64) and (days_diff.days < 91)):
                tenor = "3M"
            elif ((days_diff.days >= 91) and (days_diff.days < 183)):
                tenor = "6M"
            elif ((days_diff.days >= 183) and (days_diff.days < 275)):
                tenor = "9M"
            elif ((days_diff.days >= 275) and (days_diff.days < 367)):
                tenor = "1Y"
            elif ((days_diff.days >= 367) and (days_diff.days < 456)):
                tenor = "15M"
            elif ((days_diff.days >= 456) and (days_diff.days < 548)):
                tenor = "18M"
            elif ((days_diff.days >= 548) and (days_diff.days < 731)):
                tenor = "2Y"
            elif ((days_diff.days >= 731) and (days_diff.days < 1096)):
                tenor = "3Y"
            elif ((days_diff.days >= 1096) and (days_diff.days < 1461)):
                tenor = "4Y"
            elif ((days_diff.days >= 1461) and (days_diff.days < 1826)):
                tenor = "5Y"
            elif ((days_diff.days >= 1826) and (days_diff.days < 2191)):
                tenor = "6Y"
            elif ((days_diff.days >= 2191) and (days_diff.days < 2556)):
                tenor = "7Y"
            elif ((days_diff.days >= 2556) and (days_diff.days < 2921)):
                tenor = "8Y"
            elif ((days_diff.days >= 2921) and (days_diff.days < 3286)):
                tenor = "9Y"
            elif ((days_diff.days >= 3286) and (days_diff.days < 3651)):
                tenor = "10Y"
            elif ((days_diff.days >= 3651) and (days_diff.days < 5476)):
                tenor = "15Y"
            elif ((days_diff.days >= 5476) and (days_diff.days < 7301)):
                tenor = "20Y"
            elif ((days_diff.days >= 7301) and (days_diff.days < 9126)):
                tenor = "25Y"
            elif ((days_diff.days >= 9126) and (days_diff.days < 10951)):
                tenor = "30Y"
            else :
                tenor = "UNCLASSIFIED"

        return tenor

    def getAttrArray(self):
        single_record_array = [self.SOURCE_COMPANY_NAME, self.FILENAME, self.FILE_ID, self.ISIN, self.TRADE_DATE,
                                       self.VENUE, self.INSTRUMENT_NAME, self.INSTRUMENT_CLASSIFICATION, self.CURRENCY,
                                        str(self.ISDA_ASSET_CLASS_ID.value), self.ISDA_ASSET_CLASS_DESC, self.CFI_ATTR_1_DESC,
                                        self.CFI_ATTR_2_DESC,self.CFI_ATTR_3_DESC, self.CFI_ATTR_4_DESC,
                                        self.CFI_ATTR_5_DESC, self.CFI_ATTR_6_DESC, self.CCY1, self.CCY2, self.CCY_PAIR,
                                        self.VALUE_DATE, self.TENOR]
        return single_record_array

    def setCFIAttr(self, cfi_attr_desc, cfi_attr_num ):
        if (cfi_attr_num == "ATTR_1"):
            self.CFI_ATTR_1_DESC = cfi_attr_desc

        if (cfi_attr_num == "ATTR_2"):
            self.CFI_ATTR_2_DESC = cfi_attr_desc

        if (cfi_attr_num == "ATTR_3"):
            self.CFI_ATTR_3_DESC = cfi_attr_desc

        if (cfi_attr_num == "ATTR_4"):
            self.CFI_ATTR_4_DESC = cfi_attr_desc

        if (cfi_attr_num == "ATTR_5"):
            self.CFI_ATTR_5_DESC = cfi_attr_desc

        if (cfi_attr_num == "ATTR_6"):
            self.CFI_ATTR_6_DESC = cfi_attr_desc

    def getAssetClassEnum(self, cfi_assetclass_map):
        cfi_group = ""
        if (self.INSTRUMENT_CLASSIFICATION != "" and self.INSTRUMENT_CLASSIFICATION!=" ") :
            cfi_group = (self.INSTRUMENT_CLASSIFICATION[:2]).upper()
            asset_class_desc = cfi_assetclass_map.get(cfi_group)
            if (asset_class_desc!=None):
                asset_class_id = (int)(asset_class_desc[2])
                return AssetClassModule.AssetClass(asset_class_id)
            else :
                return AssetClassModule.AssetClass.UNCLASSIFIED
        else:
            return AssetClassModule.AssetClass.BLANK_AT_SOURCE

    def getCFIAttr_3_Desc(self, cfi_char_map):
        cfi_group = ""
        attribute_desc = "Unclassified"
        if (self.INSTRUMENT_CLASSIFICATION != ""):
            cfi_group = self.INSTRUMENT_CLASSIFICATION[:2]
            for x in cfi_char_map:
                if ((x[0]==cfi_group) and (x[1] == "ATTR_3") and (x[3]==self.INSTRUMENT_CLASSIFICATION[2])):
                      attribute_desc = x[4]
        return attribute_desc

    def getCFIAttr_4_Desc(self, cfi_char_map):
        cfi_group = ""
        attribute_desc = "Unclassified"
        if (self.INSTRUMENT_CLASSIFICATION != ""):
            cfi_group = self.INSTRUMENT_CLASSIFICATION[:2]
            for x in cfi_char_map:
                if ((x[0]==cfi_group) and (x[1] == "ATTR_4") and (x[3]==self.INSTRUMENT_CLASSIFICATION[3])):
                      attribute_desc = x[4]
        return attribute_desc

    def getCFIAttr_5_Desc(self, cfi_char_map):
        cfi_group = ""
        attribute_desc = "Unclassified"
        if (self.INSTRUMENT_CLASSIFICATION != ""):
            cfi_group = self.INSTRUMENT_CLASSIFICATION[:2]
            for x in cfi_char_map:
                if ((x[0]==cfi_group) and (x[1] == "ATTR_5") and (x[3]==self.INSTRUMENT_CLASSIFICATION[4])):
                      attribute_desc = x[4]
        return attribute_desc

    def getCFIAttr_6_Desc(self, cfi_char_map):
        cfi_group = ""
        attribute_desc = "Unclassified"
        if (self.INSTRUMENT_CLASSIFICATION != ""):
            cfi_group = self.INSTRUMENT_CLASSIFICATION[:2]
            for x in cfi_char_map:
                if ((x[0]==cfi_group) and (x[1] == "ATTR_6") and (x[3]==self.INSTRUMENT_CLASSIFICATION[5])):
                      attribute_desc = x[4]
        return attribute_desc

class RTS27_Table6:

    # Define list of attributes
    SOURCE_COMPANY_NAME = ""
    FILENAME = ""
    FILE_ID = ""
    ISIN = ""
    TRADE_DATE = ""
    INSTRUMENT_NAME =""
    NUMBER_OF_ORDER_OR_REQUEST_FOR_QUOTE = 0.0
    NUMBER_OF_TRANSACTIONS_EXECUTED = 0
    TOTAL_VALUE_OF_TRANSACTIONS_EXECUTED = 0.00
    NUMBER_OF_ORDERS_OR_REQUEST_CANCELLED_OR_WITHDRAWN = 0
    NUMBER_OF_ORDERS_OR_REQUEST_MODIFIED = 0
    MEDIAN_TRANSACTION_SIZE = 0.00
    MEDIAN_SIZE_OF_ALL_ORDERS_OR_REQUESTS_FOR_QUOTE = 0.0
    NUMBER_OF_DESIGNATED_MARKET_MAKER = 0.0
    CURRENCY = ""

    def is_number(self,s):
        try:
            float(s)
            return True
        except ValueError:
            return False

    def setSourceCompanyName(self, SOURCE_COMPANY_NAME):
        self.SOURCE_COMPANY_NAME = SOURCE_COMPANY_NAME

    def setISIN(self, ISIN):
        self.ISIN = ISIN

    def setFileName(self, FILENAME):
        self.FILENAME = FILENAME

    def setFileId(self, FILE_ID):
        self.FILE_ID = FILE_ID

    def setTradeDate (self, TRADE_DATE) :
        self.TRADE_DATE = TRADE_DATE

    def setInstrumentName(self, INSTRUMENT_NAME):
        self.INSTRUMENT_NAME = INSTRUMENT_NAME[:255]

    def setNumberOfOrderOrRequestForQuote (self, NUMBER_OF_ORDER_OR_REQUEST_FOR_QUOTE) :
        if (NUMBER_OF_ORDER_OR_REQUEST_FOR_QUOTE == ''):
            print "found a blank one"

        if (NUMBER_OF_ORDER_OR_REQUEST_FOR_QUOTE != ""
            and NUMBER_OF_ORDER_OR_REQUEST_FOR_QUOTE != " "
            and NUMBER_OF_ORDER_OR_REQUEST_FOR_QUOTE != "N/A"
            and self.is_number(NUMBER_OF_ORDER_OR_REQUEST_FOR_QUOTE)) :
            self.NUMBER_OF_ORDER_OR_REQUEST_FOR_QUOTE = NUMBER_OF_ORDER_OR_REQUEST_FOR_QUOTE

        else :
            self.NUMBER_OF_ORDER_OR_REQUEST_FOR_QUOTE = 0

    def setNumberOfTransactionsExecuted (self, NUMBER_OF_TRANSACTIONS_EXECUTED) :
        if (NUMBER_OF_TRANSACTIONS_EXECUTED != ""
            and NUMBER_OF_TRANSACTIONS_EXECUTED != " "
            and NUMBER_OF_TRANSACTIONS_EXECUTED != "N/A"
            and self.is_number(NUMBER_OF_TRANSACTIONS_EXECUTED)):
            self.NUMBER_OF_TRANSACTIONS_EXECUTED = NUMBER_OF_TRANSACTIONS_EXECUTED
        else :
            self.NUMBER_OF_TRANSACTIONS_EXECUTED = 0

    def setTotalValueOfTransactionsExecuted (self, TOTAL_VALUE_OF_TRANSACTIONS_EXECUTED) :
        if (TOTAL_VALUE_OF_TRANSACTIONS_EXECUTED != ""
            and TOTAL_VALUE_OF_TRANSACTIONS_EXECUTED != " "
            and TOTAL_VALUE_OF_TRANSACTIONS_EXECUTED != "N/A"
            and self.is_number(TOTAL_VALUE_OF_TRANSACTIONS_EXECUTED)) :
            self.TOTAL_VALUE_OF_TRANSACTIONS_EXECUTED = TOTAL_VALUE_OF_TRANSACTIONS_EXECUTED
        else :
            self.TOTAL_VALUE_OF_TRANSACTIONS_EXECUTED = 0.0

    def setNumberOfOrdersOrRequestCancelledOrWithdrawn (self, NUMBER_OF_ORDERS_OR_REQUEST_CANCELLED_OR_WITHDRAWN) :
        if (NUMBER_OF_ORDERS_OR_REQUEST_CANCELLED_OR_WITHDRAWN !=""
            and NUMBER_OF_ORDERS_OR_REQUEST_CANCELLED_OR_WITHDRAWN !=" "
            and NUMBER_OF_ORDERS_OR_REQUEST_CANCELLED_OR_WITHDRAWN != "N/A"
            and self.is_number(NUMBER_OF_ORDERS_OR_REQUEST_CANCELLED_OR_WITHDRAWN)):
            self.NUMBER_OF_ORDERS_OR_REQUEST_CANCELLED_OR_WITHDRAWN = NUMBER_OF_ORDERS_OR_REQUEST_CANCELLED_OR_WITHDRAWN
        else :
            self.NUMBER_OF_ORDERS_OR_REQUEST_CANCELLED_OR_WITHDRAWN = 0

    def setNumberOfOrdersOrRequestModified (self, NUMBER_OF_ORDERS_OR_REQUEST_MODIFIED) :
        if (NUMBER_OF_ORDERS_OR_REQUEST_MODIFIED != ""
            and NUMBER_OF_ORDERS_OR_REQUEST_MODIFIED != " "
            and NUMBER_OF_ORDERS_OR_REQUEST_MODIFIED != "N/A"
            and self.is_number(NUMBER_OF_ORDERS_OR_REQUEST_MODIFIED)) :
            self.NUMBER_OF_ORDERS_OR_REQUEST_MODIFIED = NUMBER_OF_ORDERS_OR_REQUEST_MODIFIED
        else :
            NUMBER_OF_ORDERS_OR_REQUEST_MODIFIED = 0

    def setMedianTransactionSize(self, MEDIAN_TRANSACTION_SIZE):
        if (MEDIAN_TRANSACTION_SIZE!=""
            and MEDIAN_TRANSACTION_SIZE!=" "
            and MEDIAN_TRANSACTION_SIZE != "N/A"
            and self.is_number(MEDIAN_TRANSACTION_SIZE)):
            self.MEDIAN_TRANSACTION_SIZE = MEDIAN_TRANSACTION_SIZE
        else :
            MEDIAN_TRANSACTION_SIZE = 0.0

    def setMedianSizeOfAllOrdersOrRequestsForQuote(self, MEDIAN_SIZE_OF_ALL_ORDERS_OR_REQUESTS_FOR_QUOTE):
        if (MEDIAN_SIZE_OF_ALL_ORDERS_OR_REQUESTS_FOR_QUOTE!=""
            and MEDIAN_SIZE_OF_ALL_ORDERS_OR_REQUESTS_FOR_QUOTE!=" "
            and MEDIAN_SIZE_OF_ALL_ORDERS_OR_REQUESTS_FOR_QUOTE != "N/A"
            and self.is_number(MEDIAN_SIZE_OF_ALL_ORDERS_OR_REQUESTS_FOR_QUOTE)) :
            self.MEDIAN_SIZE_OF_ALL_ORDERS_OR_REQUESTS_FOR_QUOTE = MEDIAN_SIZE_OF_ALL_ORDERS_OR_REQUESTS_FOR_QUOTE
        else :
            self.MEDIAN_SIZE_OF_ALL_ORDERS_OR_REQUESTS_FOR_QUOTE = 0.0

    def setNumberOfDesignatedMarketMaker (self, NUMBER_OF_DESIGNATED_MARKET_MAKER) :
        if (NUMBER_OF_DESIGNATED_MARKET_MAKER != ""
            and NUMBER_OF_DESIGNATED_MARKET_MAKER != " "
            and NUMBER_OF_DESIGNATED_MARKET_MAKER != "N/A"
            and self.is_number(NUMBER_OF_DESIGNATED_MARKET_MAKER)):
            self.NUMBER_OF_DESIGNATED_MARKET_MAKER = NUMBER_OF_DESIGNATED_MARKET_MAKER
        else :
            self.NUMBER_OF_DESIGNATED_MARKET_MAKER = 0

    def setCurrency (self, CURRENCY) :
        self.CURRENCY = CURRENCY

    # Constructor with params
    def setAttributes(SOURCE_COMPANY_NAME, FILENAME,FILE_ID, ISIN, TRADE_DATE, INSTRUMENT_NAME,
                 NUMBER_OF_ORDER_OR_REQUEST_FOR_QUOTE, NUMBER_OF_TRANSACTIONS_EXECUTED,
                 TOTAL_VALUE_OF_TRANSACTIONS_EXECUTED, NUMBER_OF_ORDERS_OR_REQUEST_CANCELLED_OR_WITHDRAWN,
                 NUMBER_OF_ORDERS_OR_REQUEST_MODIFIED, MEDIAN_TRANSACTION_SIZE,
                 MEDIAN_SIZE_OF_ALL_ORDERS_OR_REQUESTS_FOR_QUOTE, NUMBER_OF_DESIGNATED_MARKET_MAKER, CURRENCY):
        self.SOURCE_COMPANY_NAME = SOURCE_COMPANY_NAME
        self.FILENAME = FILENAME
        self.FILE_ID = FILE_ID
        self.ISIN = ISIN
        self.TRADE_DATE = TRADE_DATE
        self.INSTRUMENT_NAME = INSTRUMENT_NAME
        self.NUMBER_OF_ORDER_OR_REQUEST_FOR_QUOTE = int(NUMBER_OF_ORDER_OR_REQUEST_FOR_QUOTE)
        self.NUMBER_OF_TRANSACTIONS_EXECUTED = int(NUMBER_OF_TRANSACTIONS_EXECUTED)
        self.TOTAL_VALUE_OF_TRANSACTIONS_EXECUTED = Decimal(TOTAL_VALUE_OF_TRANSACTIONS_EXECUTED)
        self.NUMBER_OF_ORDERS_OR_REQUEST_CANCELLED_OR_WITHDRAWN = int(NUMBER_OF_ORDERS_OR_REQUEST_CANCELLED_OR_WITHDRAWN)
        self.NUMBER_OF_ORDERS_OR_REQUEST_MODIFIED = int(NUMBER_OF_ORDERS_OR_REQUEST_MODIFIED)
        self.MEDIAN_TRANSACTION_SIZE = Decimal(MEDIAN_TRANSACTION_SIZE)
        self.MEDIAN_SIZE_OF_ALL_ORDERS_OR_REQUESTS_FOR_QUOTE = Decimal(MEDIAN_SIZE_OF_ALL_ORDERS_OR_REQUESTS_FOR_QUOTE)
        self.NUMBER_OF_DESIGNATED_MARKET_MAKER = int(NUMBER_OF_DESIGNATED_MARKET_MAKER.replace('NULL','0'))
        self.CURRENCY = CURRENCY
        return self

    def getAttrArray(self):
        single_record_array = [ self.SOURCE_COMPANY_NAME,
                                self.FILENAME,
                                self.FILE_ID,
                                self.ISIN,
                                self.TRADE_DATE,
                                self.INSTRUMENT_NAME,
                                self.NUMBER_OF_ORDER_OR_REQUEST_FOR_QUOTE,
                                self.NUMBER_OF_TRANSACTIONS_EXECUTED,
                                self.TOTAL_VALUE_OF_TRANSACTIONS_EXECUTED,
                                self.NUMBER_OF_ORDERS_OR_REQUEST_CANCELLED_OR_WITHDRAWN,
                                self.NUMBER_OF_ORDERS_OR_REQUEST_MODIFIED,
                                self.MEDIAN_TRANSACTION_SIZE,
                                self.MEDIAN_SIZE_OF_ALL_ORDERS_OR_REQUESTS_FOR_QUOTE,
                                self.NUMBER_OF_DESIGNATED_MARKET_MAKER,
                                self.CURRENCY ]
        return single_record_array


class RTS27_Table4:

    # Define list of attributes
    SOURCE_COMPANY_NAME = ""
    FILENAME = ""
    FILE_ID = ""
    ISIN = ""
    TRADE_DATE = ""
    INSTRUMENT_NAME =""
    SIMPLE_AVERAGE_TRANSACTION_PRICE = 0.00
    VOLUME_WEIGHTED_TRANSACTION_PRICE = 0.00
    HIGHEST_EXECUTED_PRICE = 0.00
    LOWEST_EXECUTED_PRICE = 0.00
    CURRENCY = ""

    #def __init__(self) :
    #    print ("calling constructor - table 6")
    
    def is_number(self,s):
        try:
            float(s)
            return True
        except ValueError:
            return False

    def setSourceCompanyName(self, SOURCE_COMPANY_NAME):
        self.SOURCE_COMPANY_NAME = SOURCE_COMPANY_NAME

    def setCurrency(self, CURRENCY):
        self.CURRENCY = CURRENCY

    def setISIN(self, ISIN):
        self.ISIN = ISIN

    def setFileName(self, FILENAME):
        self.FILENAME = FILENAME

    def setFileId(self, FILE_ID):
        self.FILE_ID = FILE_ID

    def setTradeDate(self, TRADE_DATE):
        self.TRADE_DATE = TRADE_DATE

    def setInstrumentName(self, INSTRUMENT_NAME):
        self.INSTRUMENT_NAME = INSTRUMENT_NAME[:255]

    def setSimpleAverageTransactionPrice(self, SIMPLE_AVERAGE_TRANSACTION_PRICE):
        if (SIMPLE_AVERAGE_TRANSACTION_PRICE != "" and SIMPLE_AVERAGE_TRANSACTION_PRICE != "N/A" and self.is_number(SIMPLE_AVERAGE_TRANSACTION_PRICE)):
            self.SIMPLE_AVERAGE_TRANSACTION_PRICE = SIMPLE_AVERAGE_TRANSACTION_PRICE
        else :
            self.SIMPLE_AVERAGE_TRANSACTION_PRICE = 0.0

    def setVolumeWeightedTransactionPrice(self, VOLUME_WEIGHTED_TRANSACTION_PRICE):
        if (VOLUME_WEIGHTED_TRANSACTION_PRICE != "" and VOLUME_WEIGHTED_TRANSACTION_PRICE != "N/A" and self.is_number(VOLUME_WEIGHTED_TRANSACTION_PRICE)):
            self.VOLUME_WEIGHTED_TRANSACTION_PRICE = VOLUME_WEIGHTED_TRANSACTION_PRICE
        else:
            self.VOLUME_WEIGHTED_TRANSACTION_PRICE = 0.0

    def setHighestExecutedPrice(self, HIGHEST_EXECUTED_PRICE):
        if (HIGHEST_EXECUTED_PRICE != "" and HIGHEST_EXECUTED_PRICE!="N/A" and self.is_number(HIGHEST_EXECUTED_PRICE)):
            self.HIGHEST_EXECUTED_PRICE = HIGHEST_EXECUTED_PRICE
        else:
            self.HIGHEST_EXECUTED_PRICE = 0.0

    def setLowestExecutedPrice(self, LOWEST_EXECUTED_PRICE):
        if (LOWEST_EXECUTED_PRICE != "" and LOWEST_EXECUTED_PRICE != "N/A" and self.is_number(LOWEST_EXECUTED_PRICE)):
            self.LOWEST_EXECUTED_PRICE = LOWEST_EXECUTED_PRICE
        else:
            self.LOWEST_EXECUTED_PRICE = 0.0

    # Constructor with params
    def setAttributes(SOURCE_COMPANY_NAME, FILENAME,FILE_ID, ISIN, TRADE_DATE, INSTRUMENT_NAME,
                        SIMPLE_AVERAGE_TRANSACTION_PRICE, VOLUME_WEIGHTED_TRANSACTION_PRICE,
                        HIGHEST_EXECUTED_PRICE, LOWEST_EXECUTED_PRICE, CURRENCY) :
        self.SOURCE_COMPANY_NAME = SOURCE_COMPANY_NAME
        self.FILENAME = FILENAME
        self.FILE_ID = FILE_ID
        self.ISIN = ISIN
        self.TRADE_DATE = TRADE_DATE
        self.INSTRUMENT_NAME = INSTRUMENT_NAME
        self.SIMPLE_AVERAGE_TRANSACTION_PRICE = Decimal(SIMPLE_AVERAGE_TRANSACTION_PRICE)
        self.VOLUME_WEIGHTED_TRANSACTION_PRICE = Decimal(VOLUME_WEIGHTED_TRANSACTION_PRICE)
        self.HIGHEST_EXECUTED_PRICE = Decimal(HIGHEST_EXECUTED_PRICE)
        self.LOWEST_EXECUTED_PRICE = Decimal(LOWEST_EXECUTED_PRICE)
        self.CURRENCY = CURRENCY
        return self

    def getAttrArray(self):
        single_record_array = [ self.SOURCE_COMPANY_NAME,
                                self.FILENAME,
                                self.FILE_ID,
                                self.ISIN,
                                self.TRADE_DATE,
                                self.INSTRUMENT_NAME,
                                self.SIMPLE_AVERAGE_TRANSACTION_PRICE,
                                self.VOLUME_WEIGHTED_TRANSACTION_PRICE,
                                self.HIGHEST_EXECUTED_PRICE,
                                self.LOWEST_EXECUTED_PRICE,
                                self.CURRENCY ]
        return single_record_array


class RTS27_Table1:
    # Define list of attributes
    SOURCE_COMPANY_GROUP_NAME=""
    SOURCE_COMPANY_NAME=""
    SOURCE_COMPANY_CODE=""
    COUNTRY_OF_COMPETENT_AUTHORITY=""
    MARKET_SEGMENT_NAME=""
    MARKET_SEGMENT_ID=""
    TRADE_DATE=""
    OUTAGES_NATURE=""
    OUTAGES_NUMBER=0
    OUTAGES_AVERAGE_DURATION=""
    SCHEDULED_AUCTION_NATURE=""
    SCHEDULED_AUCTION_NUMBER=0
    SCHEDULED_AUCTION_AVERAGE_DURATION=""
    FAILED_TRANSACTIONS_NUMBER=0
    FAILED_TRANSACTIONS_PERCENT=0.00
    FILENAME=""
    FILE_ID=""
    ISIN=""
    INSTRUMENT_NAME=""
    INSTRUMENT_CLASSIFICATION=""
    CURRENCY=""

    def is_number(self, s):
        try:
            float(s)
            return True
        except ValueError:
            return False

    def setSourceCompanyName(self, SOURCE_COMPANY_NAME):
        self.SOURCE_COMPANY_NAME = SOURCE_COMPANY_NAME

    def setSourceCompanyGroupName(self, SOURCE_COMPANY_GROUP_NAME):
        self.SOURCE_COMPANY_GROUP_NAME = SOURCE_COMPANY_GROUP_NAME

    def setSourceCompanyCode(self, SOURCE_COMPANY_CODE):
        self.SOURCE_COMPANY_CODE = SOURCE_COMPANY_CODE

    def setCountryOfCompetentAuthority(self, COUNTRY_OF_COMPETENT_AUTHORITY):
        self.COUNTRY_OF_COMPETENT_AUTHORITY = COUNTRY_OF_COMPETENT_AUTHORITY

    def setMarketSegmentName(self, MARKET_SEGMENT_NAME):
        self.MARKET_SEGMENT_NAME = MARKET_SEGMENT_NAME

    def setMarketSegmentID(self, MARKET_SEGMENT_ID):
        self.MARKET_SEGMENT_ID = MARKET_SEGMENT_ID

    def setTradeDate(self, TRADE_DATE):
        self.TRADE_DATE = TRADE_DATE

    def setOutagesNature(self, OUTAGES_NATURE):
        self.OUTAGES_NATURE = OUTAGES_NATURE

    def setOutagesNumber(self, OUTAGES_NUMBER):
        if (OUTAGES_NUMBER != "" and OUTAGES_NUMBER != "N/A"
            and OUTAGES_NUMBER != " " and OUTAGES_NUMBER!=None and OUTAGES_NUMBER!="NULL" ):
            if (self.is_number(OUTAGES_NUMBER)):
                self.OUTAGES_NUMBER = OUTAGES_NUMBER
        else:
            self.OUTAGES_NUMBER = 0

    def setOutagesAverageDuration(self, OUTAGES_AVERAGE_DURATION):
        self.OUTAGES_AVERAGE_DURATION = OUTAGES_AVERAGE_DURATION

    def setScheduledAutionNature(self, SCHEDULED_AUCTION_NATURE):
        self.SCHEDULED_AUCTION_NATURE = SCHEDULED_AUCTION_NATURE

    def setScheduledAutionNumber(self, SCHEDULED_AUCTION_NUMBER):
        self.SCHEDULED_AUCTION_NUMBER = SCHEDULED_AUCTION_NUMBER

    def setScheduledAutionAverageDuration(self, SCHEDULED_AUCTION_AVERAGE_DURATION):
        self.SCHEDULED_AUCTION_AVERAGE_DURATION = SCHEDULED_AUCTION_AVERAGE_DURATION

    def setFailedTransactionsNumber(self, FAILED_TRANSACTIONS_NUMBER):
        if (FAILED_TRANSACTIONS_NUMBER != "" and FAILED_TRANSACTIONS_NUMBER != "N/A"
            and FAILED_TRANSACTIONS_NUMBER != " " and FAILED_TRANSACTIONS_NUMBER!=None and FAILED_TRANSACTIONS_NUMBER!="NULL" ):
            if (self.is_number(FAILED_TRANSACTIONS_NUMBER)):
                self.FAILED_TRANSACTIONS_NUMBER = FAILED_TRANSACTIONS_NUMBER
        else:
            self.FAILED_TRANSACTIONS_NUMBER = 0

    def setFailedTransactionsPercent(self, FAILED_TRANSACTIONS_PERCENT):
        if (FAILED_TRANSACTIONS_PERCENT != "" and FAILED_TRANSACTIONS_PERCENT != "N/A" and FAILED_TRANSACTIONS_PERCENT != "NULL"
                and self.is_number(FAILED_TRANSACTIONS_PERCENT)):
            self.FAILED_TRANSACTIONS_PERCENT = FAILED_TRANSACTIONS_PERCENT
        else:
            self.FAILED_TRANSACTIONS_PERCENT = 0.0

    def setCurrency(self, CURRENCY):
        self.CURRENCY = CURRENCY

    def setISIN(self, ISIN):
        self.ISIN = ISIN

    def setFileName(self, FILENAME):
        self.FILENAME = FILENAME

    def setFileId(self, FILE_ID):
        self.FILE_ID = FILE_ID

    def setInstrumentName(self, INSTRUMENT_NAME):
        self.INSTRUMENT_NAME = INSTRUMENT_NAME

    def setInstrumentClassification(self, INSTRUMENT_CLASSIFICATION):
        self.INSTRUMENT_CLASSIFICATION = INSTRUMENT_CLASSIFICATION

    def getAttrArrayTable1(self):
        single_record_array = [
                               self.SOURCE_COMPANY_GROUP_NAME,
                               self.SOURCE_COMPANY_NAME,
                               self.SOURCE_COMPANY_CODE,
                               self.COUNTRY_OF_COMPETENT_AUTHORITY,
                               self.MARKET_SEGMENT_NAME,
                               self.MARKET_SEGMENT_ID,
                               self.TRADE_DATE,
                               self.OUTAGES_NATURE,
                               self.OUTAGES_NUMBER,
                               self.OUTAGES_AVERAGE_DURATION,
                               self.SCHEDULED_AUCTION_NATURE,
                               self.SCHEDULED_AUCTION_NUMBER,
                               self.SCHEDULED_AUCTION_AVERAGE_DURATION,
                               self.FAILED_TRANSACTIONS_NUMBER,
                               self.FAILED_TRANSACTIONS_PERCENT,
                               self.FILENAME,
                               self.FILE_ID,
                               self.ISIN,
                               self.INSTRUMENT_NAME,
                               self.INSTRUMENT_CLASSIFICATION,
                               self.CURRENCY]
        return single_record_array