#!/usr/bin/python

import pymysql, sys
import logging
from decimal import Decimal

class RTS27_LEI_Company_Map:

    def getMap(self):
        lei_comp_map = {
            "HPFHU0OQ28E4N0NFVK49": "The Bank of New York Mellon",
            "MMYX0N4ZEZ13Z4XCG897": "THE BANK OF NEW YORK MELLON SA/NV",
            "1ZU7M6R6N6PXYJ6V0C83": "Kyte Broking Limited",
            "213800KL2QZT2GQMQQ34" : "LOUIS CAPITAL MARKETS UK LLP",
            "549300NQ588N7RWKBP98" : "OP Yrityspankki Oyj",
            "549300R5V1LQ6NX1W326" : "OP-Palvelut Oy",
            "571474TGEMMWANRLN572" : "State Street Bank and Trust Company",
            "5493000YPN33HF74SN02" : "Bank of Montreal Ireland Public Limited Company",
            "529900032TYR45XIEW79" : "EUWAX Aktiengesellschaft",
            "ES7IP3U3RHIGC71XBU11" : "Royal Bank of Canada",
            "JHE42UYNWWTJB8YTTU19" : "Australia and New Zealand Banking Group Limited",
            "NQQ6HPCNCCU6TUTQYE16" : "Bank of Montreal",
            "TXDSU46SXBWIGJ8G8E98" : "RBC Europe Limited",
            "ZMHGNT7ZPKZ3UFZ8EO46" : "State Street Bank International GmbH"
        }
        return lei_comp_map
