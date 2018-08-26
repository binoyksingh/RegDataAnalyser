import logging
import sys
import tabula
import pandas

file_path = '/Users/sarthakagarwal/PycharmProjects/MifidDataAnalyser/Source/RTS28/2017_Ruffer_top5_execution_venues.pdf'
df = tabula.read_pdf(file_path, encoding='utf-8', pages='1-13')
df.to_csv('/Users/sarthakagarwal/PycharmProjects/MifidDataAnalyser/Source/RTS28/output.csv', encoding='utf-8')


print "Test to standard out"
