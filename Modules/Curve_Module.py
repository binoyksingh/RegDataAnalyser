#!/usr/bin/python

import sys
from decimal import Decimal
import numpy as np
from scipy import interpolate
from scipy.interpolate import interp1d

class Curve:

    # Define list of attributes
    CurvePointList=[]
    CurveType = ""

    def __init__(self) :
        self.CurvePointList = []
        self.CurveType=""

    def setCurveType(self, curve_point):
        self.CurveType = curve_point

    def addCurvePoint(self, curve_point):
        self.CurvePointList.append(curve_point)

    def getMktDataDetailXML(self):
        mkt_data_detail_xml = ""
        mkt_data_detail_xml += "<"+ self.CurveType + ">"
        for curve_point in self.CurvePointList:
            mkt_data_detail_xml += "<CurvePoint>"
            mkt_data_detail_xml += "<Tenor>" + curve_point.TENOR + "</Tenor>"
            mkt_data_detail_xml += "<MidPrice>" + curve_point.MIDPRICE + "</MidPrice>"
            mkt_data_detail_xml += "</CurvePoint>"
        mkt_data_detail_xml += "</"+ self.CurveType + ">"
        return mkt_data_detail_xml

    def getPriceForTenor(self, tenor_days):
        x_axis_points = []
        y_axis_points = []
        for curve_point in self.CurvePointList:
            x_axis_points.append(self.getDaysInTenor(curve_point.TENOR) )
            y_axis_points.append(Decimal(curve_point.MIDPRICE))

        x2 = np.linspace(0, 360, 361)
        # Currently the plots are only plotted to 361 days, so we need to ignore
        # any trade with the tenor_days of over 361 days.
        interp_val_linear = 0
        if (int(tenor_days.days)<361):
            if (len(x_axis_points)>1) :
                 f_cubic = interp1d(x_axis_points, y_axis_points, kind='cubic')
                 temp_cubic = f_cubic(x2)
                 interp_val_linear = temp_cubic[int(tenor_days.days)]
        return interp_val_linear

    def getDaysInTenor(self, tenor):
        tenor_days = 0
        if "M" in tenor:
            tenor_months = int(tenor.replace("M", ""))
            tenor_days = int(tenor_months) * 30
        return tenor_days

