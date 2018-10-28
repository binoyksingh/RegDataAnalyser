#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Oct  9 22:14:30 2018

@author: lojinilogesparan
"""
import csv
import numpy as np
import matplotlib.pyplot as plt

filename = '/Users/lojinilogesparan/Documents/mifid_data/Competition Analysis Stats - BNYM_NEW.csv'
currYear = 2018

# Load data
month = []
company = []
curr = []
fxrate = []
currpair = []
tenor = []
nominalUSD = []
tradecount = []
tradedate = []
rowCount = 0
with open(filename, 'rU') as csvfile:
    csvreader = csv.reader(csvfile)
    for row in csvreader:
        if rowCount > 0 and (row[len(row)-1])==str(0):
            month.append(int(row[0]))
            company.append(row[1])
            curr.append(row[3])
            fxrate.append(row[4])
            tmpcurrpair = row[5]
            currpair.append(tmpcurrpair[1:])
            tenor.append(row[6])
            tradedate.append(row[7])
            nominalUSD.append(float(row[9]))
            tradecount.append(int(row[10]))
        rowCount += 1

#%% Trading activity by date
# swap date format
newtradedate = []
for ind in range(0,len(tradedate)):
    currdate  =tradedate[ind]
    newtradedate.append(currdate[3:5]+'/'+currdate[:2]+'/20'+currdate[-2:])
        
uniqueDates = np.unique(newtradedate)
uniqueDates.sort()

nominalUSDByDate = np.zeros(len(uniqueDates))
tradeCountByDate = np.zeros(len(uniqueDates))
for ind in range(0,len(uniqueDates)):
    for date in range(0,len(newtradedate)):
        if newtradedate[date] == uniqueDates[ind]:
            nominalUSDByDate[ind] += nominalUSD[date]
            tradeCountByDate[ind] += tradecount[date]

#%% Calculate all transactions per month
monthbymonth = np.unique(month)
sumNominalUSD = np.zeros(len(monthbymonth))
sumTradeCount = np.zeros(len(monthbymonth))

for ind in range(0,len(monthbymonth)):
    # Match all data to months
    for subind in range(0,len(month)):
        if month[subind] == monthbymonth[ind]:
            sumNominalUSD[ind] += nominalUSD[subind]
            sumTradeCount[ind] += tradecount[subind]
            
#%% Aggregate by currency pair (month-wise change)
uniqueCurrPair = np.unique(currpair)
sumTradeCountByMonth = np.zeros([len(monthbymonth),len(uniqueCurrPair)])
sumTradeNominalByMonth = np.zeros([len(monthbymonth),len(uniqueCurrPair)])
percChangeByMonth = np.zeros([len(monthbymonth),len(uniqueCurrPair)])

for currInd in range(0,len(uniqueCurrPair)):
    for monthInd in range(0,len(monthbymonth)):
        for ind in range(0,len(currpair)):
            if (currpair[ind] == uniqueCurrPair[currInd]) and (month[ind]==monthbymonth[monthInd]):
                sumTradeCountByMonth[monthInd,currInd] += tradecount[ind]
                sumTradeNominalByMonth[monthInd,currInd] += nominalUSD[ind]
        if monthInd > 0:
            percChangeByMonth[monthInd,currInd] = sumTradeCountByMonth[monthInd,currInd] - sumTradeCountByMonth[monthInd-1,currInd]

#%% Popular tenor per currency pair
uniqueTenor = np.unique(tenor)
tradeCountByTenor = np.zeros([len(uniqueTenor),len(uniqueCurrPair)])
tradeNominaByTenor = np.zeros([len(uniqueTenor),len(uniqueCurrPair)])
freqTenorbyCurrPair = []
for currInd in range(0,len(uniqueCurrPair)):
    for tenorInd in range(0,len(uniqueTenor)):
        for ind in range(0,len(tenor)):
            if uniqueTenor[tenorInd] == tenor[ind] and uniqueCurrPair[currInd]==currpair[ind]:
                tradeCountByTenor[tenorInd,currInd] += tradecount[ind]
                tradeNominaByTenor[tenorInd,currInd] += nominalUSD[ind]
    # Highest tenor traded by volume per currency pair
    freqTenorbyCurrPair.append(uniqueTenor[np.argmax(tradeCountByTenor[:,currInd])])

#%% Big into currency pair buckets
g10Curr = ['USD','EUR','GBP','JPY','AUD','NZD','CAD','CHF','NOK','SEK']
skandinavian = ['DKK','SEK','NOK'] # Finland uses Euro; Iceland not included
emergingMarket = ['BRL','RUB','INR','CNH','ZAR','ARS','CLF','CLP','MXN','TRY'] 

g10TradeCount = []
g10Nominal = []
g10month = []
skanTradeCount = []
skanNominal = []
skanmonth = []
emerTradeCount = []
emerNominal = []
emermonth = []
for ind in range(0,len(currpair)):
    for tmp in range(0,len(g10Curr)):
        comp = currpair[ind]
        if (g10Curr[tmp] == comp[:2]) or (g10Curr[tmp] == comp[3:]):
            g10TradeCount.append(tradecount[ind])
            g10Nominal.append(nominalUSD[ind])
            g10month.append(month[ind])
            
    for tmp in range(0,len(skandinavian)):
        comp = currpair[ind]
        if (skandinavian[tmp] == comp[:2]) or (skandinavian[tmp] == comp[3:]):
            skanTradeCount.append(tradecount[ind])
            skanNominal.append(nominalUSD[ind])
            skanmonth.append(month[ind])
            
    for tmp in range(0,len(emergingMarket)):
        comp = currpair[ind]
        if (emergingMarket[tmp] == comp[:2]) or (emergingMarket[tmp] == comp[3:]):
            emerTradeCount.append(tradecount[ind])
            emerNominal.append(nominalUSD[ind])
            emermonth.append(month[ind])
    
# collate by month
g10TradeCountByMonth = np.zeros(len(monthbymonth))
g10NominalByMonth = np.zeros(len(monthbymonth))
for ind in range(0,len(g10month)):
    g10TradeCountByMonth[g10month[ind]-1] += g10TradeCount[ind]
    g10NominalByMonth[g10month[ind]-1] += g10Nominal[ind]

skanTradeCountByMonth = np.zeros(len(monthbymonth))
skanNominalByMonth = np.zeros(len(monthbymonth))
for ind in range(0,len(skanmonth)):
    skanTradeCountByMonth[skanmonth[ind]-1] += skanTradeCount[ind]
    skanNominalByMonth[skanmonth[ind]-1] += skanNominal[ind]

emergTradeCountByMonth = np.zeros(len(monthbymonth))
emergNominalByMonth = np.zeros(len(monthbymonth))
for ind in range(0,len(emermonth)):
    emergTradeCountByMonth[emermonth[ind]-1] += emerTradeCount[ind]
    emergNominalByMonth[emermonth[ind]-1] += emerNominal[ind]
    

#%% Trends of highest trade count or nominal USD value

# aggregate by currpair and tenor
infoByCurrPair = {}
for currInd in range(0,len(uniqueCurrPair)):
    for tenorID in range(0,len(uniqueTenor)):
        tmpTradeCount = []
        tmpNominalUSD = []
        tmpMonth = []
        tmpDate = []
        for ind in range(0,len(tradecount)):
            if currpair[ind] == uniqueCurrPair[currInd] and tenor[ind] == uniqueTenor[tenorID]:    
                tmpTradeCount.append(tradecount[ind])
                tmpNominalUSD.append(nominalUSD[ind])
                tmpMonth.append(month[ind])
                tmpDate.append(tradedate[ind])
        
        # update dictionary           
        if uniqueCurrPair[currInd] not in infoByCurrPair.keys():
            infoByCurrPair.update({uniqueCurrPair[currInd]:{}})
        infoByCurrPair[uniqueCurrPair[currInd]].update({uniqueTenor[tenorID]:{}})
        infoByCurrPair[uniqueCurrPair[currInd]][uniqueTenor[tenorID]].update({'tradecount':tmpTradeCount})
        infoByCurrPair[uniqueCurrPair[currInd]][uniqueTenor[tenorID]].update({'nominalUSD':tmpNominalUSD})
        infoByCurrPair[uniqueCurrPair[currInd]][uniqueTenor[tenorID]].update({'month':tmpMonth})
        infoByCurrPair[uniqueCurrPair[currInd]][uniqueTenor[tenorID]].update({'tradedate':tmpDate})
    
# trading pattern per month
for currKey in infoByCurrPair.keys():
    for tenorKey in infoByCurrPair[currKey].keys():
        if len(infoByCurrPair[currKey][tenorKey]['nominalUSD'])>=3:
            tmpDate = infoByCurrPair[currKey][tenorKey]['tradedate']
            for DateInd in range(1,32):
                # check if same trade happens on same day in more than one month
                count = 0
                for monthInd in range(1,13):
                    currDate = str(currYear)+'-'+str(monthInd)+'-'+str(DateInd)
                    notMatched = 0
                    ind = 0
                    while ind < len(tmpDate) or notMatched:
                        if tmpDate[ind] == currDate:
                            notMatched = 1
                            count += 1
                        ind += 1
                # pattern occurs in 3 of 12 months
                if count >= 3:
                    print(currKey+"/"+tenorKey)
                
"""
# monthly trading activity    
avgTradeCountPerMonth = np.zeros([len(uniqueCurrPair),len(uniqueTenor),len(monthbymonth)])
stdTradeCountPerMonth = np.zeros([len(uniqueCurrPair),len(uniqueTenor),len(monthbymonth)])
maxTradeCountPerMonth = np.zeros([len(uniqueCurrPair),len(uniqueTenor),len(monthbymonth)])
avgNominalPerMonth = np.zeros([len(uniqueCurrPair),len(uniqueTenor),len(monthbymonth)])
stdNominalPerMonth = np.zeros([len(uniqueCurrPair),len(uniqueTenor),len(monthbymonth)])
maxNominalPerMonth = np.zeros([len(uniqueCurrPair),len(uniqueTenor),len(monthbymonth)])

for currInd in range(0,len(uniqueCurrPair)):
    for tenorID in range(0,len(uniqueTenor)):
        # data per currpair
        tmpMonth = infoByCurrPair[uniqueCurrPair[currInd]][uniqueTenor[tenorID]]['month'] 
        tmpTradeCount = infoByCurrPair[uniqueCurrPair[currInd]][uniqueTenor[tenorID]]['tradecount'] 
        tmpNominalUSD = infoByCurrPair[uniqueCurrPair[currInd]][uniqueTenor[tenorID]]['nominalUSD'] 
        for monthID in range(0,len(monthbymonth)):
            # initialise month on month temp variables
            tradeCountByMonth = []
            tradeNominalByMonth = []
            for ind in range(0,len(tmpMonth)):
                if tmpMonth[ind] == monthbymonth[monthID]:
                    tradeCountByMonth.append(tmpTradeCount[ind])
                    tradeNominalByMonth.append(tmpNominalUSD[ind])
            
            # aggregate per month
            if len(tradeCountByMonth) > 0:
                avgTradeCountPerMonth[currInd,tenorID,monthID] = np.average(tradeCountByMonth)
                stdTradeCountPerMonth[currInd,tenorID,monthID] = np.std(tradeCountByMonth)
                maxTradeCountPerMonth[currInd,tenorID,monthID] = np.max(tradeCountByMonth)
                avgNominalPerMonth[currInd,tenorID,monthID] = np.average(tradeNominalByMonth)
                stdNominalPerMonth[currInd,tenorID,monthID] = np.std(tradeNominalByMonth)
                maxNominalPerMonth[currInd,tenorID,monthID] = np.max(tradeNominalByMonth)
            
            
# Identify currencies where large trade count occur every month
tradingSpikes = {}
for ind in range(0,len(uniqueCurrPair)):
    tradingDateForSpike = []
    for tenorID in range(0,len(uniqueTenor)):
        monthCount = 0
        for monthID in range(0,len(monthbymonth)):
            if maxTradeCountPerMonth[ind,tenorID,monthID] > avgTradeCountPerMonth[ind,tenorID,monthID] + 2*stdTradeCountPerMonth[ind,tenorID,monthID]:
                monthCount += 1
                # match trade date
                tradeValue = infoByCurrPair[uniqueCurrPair[ind]][uniqueTenor[tenorID]]['nominalUSD']
                tradeMonth = infoByCurrPair[uniqueCurrPair[ind]][uniqueTenor[tenorID]]['month']
                matchedTrade = np.argmin(np.abs(tradeValue - maxTradeCountPerMonth[ind,tenorID,monthID]))
                if tradeMonth[matchedTrade] == monthbymonth[monthID]:
                    tradingDateForSpike.append(tradedate[matchedTrade])
                
        # if patterns exists for over a quarter and repeated in last month
        if (monthCount >= 3) and (maxTradeCountPerMonth[ind,tenorID,monthID] > avgTradeCountPerMonth[ind,tenorID,monthID] + 2*stdTradeCountPerMonth[ind,tenorID,monthID]):
            incTradeCount = 100*(maxTradeCountPerMonth[ind,tenorID,monthID]-avgTradeCountPerMonth[ind,tenorID,monthID])/avgTradeCountPerMonth[ind,tenorID,monthID]
            if uniqueCurrPair[ind] not in tradingSpikes.keys():
                tradingSpikes.update({uniqueCurrPair[ind]:{}})
            tradingSpikes[uniqueCurrPair[ind]].update({uniqueTenor[tenorID]:{}})
            tradingSpikes[uniqueCurrPair[ind]][uniqueTenor[tenorID]].update({'TradeIncCount': incTradeCount})
            tradingSpikes[uniqueCurrPair[ind]][uniqueTenor[tenorID]].update({'MaxTradeCount':maxTradeCountPerMonth[ind,tenorID,:]})
            tradingSpikes[uniqueCurrPair[ind]][uniqueTenor[tenorID]].update({'TradeDateCount':tradingDateForSpike})

# Identify currencies where large nominal value occur every month
for ind in range(0,len(uniqueCurrPair)):
    tradingDateForSpike = []
    for tenorID in range(0,len(uniqueTenor)):
        monthCount = 0
        for monthID in range(0,len(monthbymonth)):
            if maxNominalPerMonth[ind,tenorID,monthID] > avgNominalPerMonth[ind,tenorID,monthID] + 2*stdNominalPerMonth[ind,tenorID,monthID]:
                monthCount += 1
                # match trade date
                tradeValue = infoByCurrPair[uniqueCurrPair[ind]][uniqueTenor[tenorID]]['nominalUSD']
                tradeMonth = infoByCurrPair[uniqueCurrPair[ind]][uniqueTenor[tenorID]]['month']
                matchedTrade = np.argmin(np.abs(tradeValue - maxTradeCountPerMonth[ind,tenorID,monthID]))
                if tradeMonth[matchedTrade] == monthbymonth[monthID]:
                    tradingDateForSpike.append(tradedate[matchedTrade])
                
        # if patterns exists for over a quarter and repeated in last month
        if (monthCount >= 3) and (maxNominalPerMonth[ind,tenorID,monthID] > avgNominalPerMonth[ind,tenorID,monthID] + 2*stdNominalPerMonth[ind,tenorID,monthID]):
            incTradeNominal = 100*(maxNominalPerMonth[ind,tenorID,monthID]-avgNominalPerMonth[ind,tenorID,monthID])/avgNominalPerMonth[ind,tenorID,monthID]
            if uniqueCurrPair[ind] not in tradingSpikes.keys():
                tradingSpikes.update({uniqueCurrPair[ind]:{}})
            if uniqueTenor[tenorID] not in tradingSpikes[uniqueCurrPair[ind]].keys():
                tradingSpikes[uniqueCurrPair[ind]].update({uniqueTenor[tenorID]:{}})
            tradingSpikes[uniqueCurrPair[ind]][uniqueTenor[tenorID]].update({'TradeIncNominal': incTradeNominal})
            tradingSpikes[uniqueCurrPair[ind]][uniqueTenor[tenorID]].update({'MaxNominal':maxNominalPerMonth[ind,tenorID,:]})
            tradingSpikes[uniqueCurrPair[ind]][uniqueTenor[tenorID]].update({'TradeDateNominal':tradingDateForSpike})
"""