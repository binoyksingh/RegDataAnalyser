#!/usr/bin/python

import pymysql, sys
import logging
from decimal import Decimal

class RTS27_LEI_Company_Map:

    def getMap(self):
        lei_comp_map = {
            "HPFHU0OQ28E4N0NFVK49": "The Bank of New York Mellon",
            "MMYX0N4ZEZ13Z4XCG897": "THE BANK OF NEW YORK MELLON SA/NV"
        }
        return lei_comp_map
