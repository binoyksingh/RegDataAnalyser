#!/usr/bin/python

import sys

class FIRDS_Data:

    # Define list of attributes
    INSTRUMENT_IDENTIFICATION_CODE=""
    INSTRUMENT_FULL_NAME=""
    INSTRUMENT_CLASSIFICATION=""
    ISSUER_IDENTIFIER=""
    TRADING_VENUE=""
    INSTRUMENT_SHORT_NAME=""
    TERMINATION_DATE=""
    NOTIONAL_CURRENCY1=""
    EXPIRY_DATE=""
    DELIVERY_TYPE=""
    FX_TYPE=""
    UNDERLYING_ISIN=""
    OPTION_TYPE=""
    STRIKE_PRICE_AMT=0.00
    OPTION_EXERCISE_STYLE=""
    RELEVANT_COMPETENT_AUTHORITY=""
    DEBT_TOTAL_ISSUED_NOMINAL_AMOUNT=0
    DEBT_MATURTY_DATE=""
    DEBT_FIXED_RATE=""
    FR_DATE=""
    FILE_NAME=""

    def setInstrumentIdentificationCode(self, INSTRUMENT_IDENTIFICATION_CODE):
        self.INSTRUMENT_IDENTIFICATION_CODE = INSTRUMENT_IDENTIFICATION_CODE

    def setInstrumentFullName(self, INSTRUMENT_FULL_NAME):
        self.INSTRUMENT_FULL_NAME = INSTRUMENT_FULL_NAME

    def setInstrumentClassification(self, INSTRUMENT_CLASSIFICATION):
        self.INSTRUMENT_CLASSIFICATION = INSTRUMENT_CLASSIFICATION

    def setIssuerIdentifier(self, ISSUER_IDENTIFIER):
        self.ISSUER_IDENTIFIER = ISSUER_IDENTIFIER

    def setTradingVenue(self, TRADING_VENUE):
        self.TRADING_VENUE = TRADING_VENUE

    def setInstrumentShortName(self, INSTRUMENT_SHORT_NAME):
        self.INSTRUMENT_SHORT_NAME = INSTRUMENT_SHORT_NAME

    def setTerminationDate(self, TERMINATION_DATE):
        if (TERMINATION_DATE!=' ' and TERMINATION_DATE!=''):

            self.TERMINATION_DATE = TERMINATION_DATE

    def setNotionalCurrency1(self, NOTIONAL_CURRENCY1):
        self.NOTIONAL_CURRENCY1 = NOTIONAL_CURRENCY1

    def setExpiryDate(self, EXPIRY_DATE):
        self.EXPIRY_DATE = None
        if (EXPIRY_DATE!='' and EXPIRY_DATE!=' '):
            self.EXPIRY_DATE = EXPIRY_DATE


    def setDeliveryType(self, DELIVERY_DATE):
        self.DELIVERY_DATE = DELIVERY_DATE

    def setFXType(self, FX_TYPE):
        self.FX_TYPE = FX_TYPE

    def setUnderlyingISIN(self, UNDERLYING_ISIN):
        self.UNDERLYING_ISIN = UNDERLYING_ISIN

    def setOptionType(self, OPTION_TYPE):
        self.OPTION_TYPE = OPTION_TYPE

    def is_number(self,s):
        try:
            float(s)
            return True
        except ValueError:
            return False

    def setStrikePriceAmt(self, STRIKE_PRICE_AMT):
        if (STRIKE_PRICE_AMT!="" and STRIKE_PRICE_AMT!=" " and STRIKE_PRICE_AMT != "N/A" and self.is_number(STRIKE_PRICE_AMT)):
            self.STRIKE_PRICE_AMT = STRIKE_PRICE_AMT
        else :
            self.STRIKE_PRICE_AMT = 0.0

    def setOptionExerciseStyle(self, OPTION_EXERCISE_STYLE):
        self.OPTION_EXERCISE_STYLE = OPTION_EXERCISE_STYLE

    def setRelevantCompetentAuthrity(self, RELEVANT_COMPETENT_AUTHORITY):
        self.RELEVANT_COMPETENT_AUTHORITY = RELEVANT_COMPETENT_AUTHORITY

    def setDebtTotalIssuedNominalAmount(self, DEBT_TOTAL_ISSUED_NOMINAL_AMOUNT):
        if (DEBT_TOTAL_ISSUED_NOMINAL_AMOUNT!='' and DEBT_TOTAL_ISSUED_NOMINAL_AMOUNT!=' '):
            self.DEBT_TOTAL_ISSUED_NOMINAL_AMOUNT = int(DEBT_TOTAL_ISSUED_NOMINAL_AMOUNT)

    def setDebtMaturityDate(self, DEBT_MATURTY_DATE):
        self.DEBT_MATURTY_DATE = None
        if (DEBT_MATURTY_DATE != '' and DEBT_MATURTY_DATE != ' '):
            self.DEBT_MATURTY_DATE = DEBT_MATURTY_DATE

    def setDebtFixedRate(self, DEBT_FIXED_RATE):
        self.DEBT_FIXED_RATE = DEBT_FIXED_RATE

    def setFRDate(self, FR_DATE):
        self.FR_DATE = None
        if (FR_DATE != '' and FR_DATE != ' '):
            self.FR_DATE = FR_DATE

    def setFileName(self, FILE_NAME):
        self.FILE_NAME = FILE_NAME

    def getAttrArray(self):
        single_record_array = [ self.INSTRUMENT_IDENTIFICATION_CODE,
                                self.INSTRUMENT_FULL_NAME,
                                self.INSTRUMENT_CLASSIFICATION,
                                self.ISSUER_IDENTIFIER,
                                self.TRADING_VENUE,
                                self.INSTRUMENT_SHORT_NAME,
                                self.TERMINATION_DATE,
                                self.NOTIONAL_CURRENCY1,
                                self.EXPIRY_DATE,
                                self.DELIVERY_TYPE,
                                self.FX_TYPE,
                                self.UNDERLYING_ISIN,
                                self.OPTION_TYPE,
                                self.STRIKE_PRICE_AMT,
                                self.OPTION_EXERCISE_STYLE,
                                self.RELEVANT_COMPETENT_AUTHORITY,
                                self.DEBT_TOTAL_ISSUED_NOMINAL_AMOUNT,
                                self.DEBT_MATURTY_DATE,
                                self.DEBT_FIXED_RATE,
                                self.FR_DATE,
                                self.FILE_NAME
                            ]

        return single_record_array