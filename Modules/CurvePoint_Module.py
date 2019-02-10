#!/usr/bin/python

import sys

class Curve_Point:

    # Define list of attributes
    MIDPRICE=""
    TENOR=""

    def setTenor(self, TENOR):
        self.TENOR = TENOR

    def setMidPrice(self, MIDPRICE):
        self.MIDPRICE = MIDPRICE

    def getAttrArray(self):
        single_record_array = [self.TENOR,
                               self.MIDPRICE]
        return single_record_array