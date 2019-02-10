#!/usr/bin/python

import pymysql, sys
import Modules.RTS27_Table_Records_Module
from collections import defaultdict

class RTS27_Prod_Class_DB_Reader:

    connection = pymysql.Connection

    def __init__(self):
        print ('RTS27_Prod_Class_DB_Reader:INIT:Calling Constructor')
        self.connection = pymysql.connect(host='35.224.67.19', user='root', password='root', db='mifid')
        self.cursor = self.connection.cursor()

        self.cfi_asset_class_map = {}
        self.cfi_char_map = []

        self.InitCfi_AssetClass_Map()
        self.InitCfi_Char_Map()
        print ('RTS27_DB_WRITER:INIT:Connection Success')

    def __del__(self):
        print ('RTS27_DB_WRITER:INIT:Calling destructor')
        self.connection.close()

    # Initialise the CFI Asset Class Map
    def InitCfi_AssetClass_Map(self):

        select_cfi_assetclass_string = "SELECT CFI_GROUP, CFI_CATEGORY_DESC, CFI_GROUP_DESC, ASSET_CLASS_ID," \
                                       "ASSET_CLASS_DESC from `CFI_ASSETCLASS_MAP`  "

        self.cursor.execute(select_cfi_assetclass_string)

        myresult =  self.cursor.fetchall()
        cfi_assetclass_map = {}

        for x in myresult:
            cfi_assetclass_map[x[0]] = [x[1],x[2],x[3],x[4]]

        self.cfi_asset_class_map = cfi_assetclass_map


    # Initialise the CFI Characters Map
    def InitCfi_Char_Map(self):

        select_cfi_char_map_string = "SELECT CFI_GROUP, CFI_ATTRIBUTE_POSITION, CFI_ATTRIBUTE_GROUP_DESC, CFI_ATTRIBUTE," \
                                       "CFI_ATTRIBUTE_DESC from `CFI_CHAR_MAP`"
        self.cursor.execute(select_cfi_char_map_string)
        self.cfi_char_map = self.cursor.fetchall()

    def getCfi_assetclass_map(self):
        return self.cfi_asset_class_map

    def getCfi_char_map(self):
        return self.cfi_char_map