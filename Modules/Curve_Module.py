#!/usr/bin/python

import sys

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
