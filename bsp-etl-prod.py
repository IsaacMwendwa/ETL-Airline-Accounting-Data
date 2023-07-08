#!/usr/bin/python

import re
# import paramiko
import os
import glob
import datetime
import platform
import time
import copy
import zipfile
from zipfile import ZipFile
from datetime import timedelta, date
import cx_Oracle
import sys

#import pandas as pd
import logging


if platform.system().upper() == 'LINUX':
    os.environ['LD_LIBRARY_PATH'] = ###Lib Path###
    os.environ['ORACLE_HOME'] = ###Oracle Home Path###
    os.environ['ORACLE_SID'] = ###Oracle SID###
    localpath = ###Local Path of Data Lake###
else:
    localpath = ###Local Path of Data Lake###
dsn_tns = cx_Oracle.makedsn(host=host, port=port, sid=sid)
dbCon = cx_Oracle.connect(user=user, password=password,
                       dsn=dsn_tns)
cursor = cx_Oracle.Cursor(dbCon)
_k = 1
rows = []
digits = frozenset('0123456789-+')



def main():

    fnames = os.listdir(localpath)
    modDate = 000000
    index = len(fnames)
    listFiles = []
    listDates = []
    filesModDate = []
    logModDate = []

    for filename in fnames:
        mfilename = os.path.basename(filename)
        aspath = localpath + mfilename
        modDateF = time.strftime('%y%m%d', time.localtime(os.path.getmtime(aspath)))
        mod_date_f = datetime.datetime.strptime(modDateF, '%y%m%d')
        mod_date_f = datetime.datetime.date(mod_date_f)
        filesModDate.append(mod_date_f)
        listDates.append(mod_date_f)

    filesModDate.sort(reverse=True)

    filesModDate = filesModDate[0]  # get last mod date
    filesModDate = (filesModDate.strftime('%y%m%d'))
    filesModDate = datetime.datetime.strptime(filesModDate, '%y%m%d')
    filesModDate = datetime.datetime.date(filesModDate)
    print(filesModDate)

    cursor.execute('SELECT MOD_DATE FROM DATABASE_NAME.V_BSP_LOG_TABLE WHERE MOD_DATE IS NOT NULL ORDER BY MOD_DATE DESC')
    latestdate = cursor.fetchone()
    #print(latestdate)
    if latestdate is None:
        listDates.sort()  # sort all files by mod date
        latestDate = listDates[0]  # get first mod date
        latestDate = (latestDate.strftime('%y%m%d'))
        latestDate = datetime.datetime.strptime(latestDate, '%y%m%d')
        latestDate = datetime.datetime.date(latestDate)
        logTableModDate = latestDate
        print(logTableModDate)
        daysCounter = (filesModDate - logTableModDate).days
    else:
        logTableModDate = latestdate[0]
        logTableModDate = (logTableModDate.strftime('%y%m%d'))
        logTableModDate = datetime.datetime.strptime(logTableModDate, '%y%m%d')
        logTableModDate = datetime.datetime.date(logTableModDate)

        daysCounter = (filesModDate - logTableModDate).days

    print(daysCounter)
    days = 0
    while days <= daysCounter:
        _mcount = 0
        while _mcount <= 0:
            # print(days)
            myesterday = datetime.datetime.now() - datetime.timedelta(days)
            # mtoday = datetime.datetime.now() - datetime.timedelta(days=0)
            myesterday = myesterday.strftime("%y%m%d")
            #print(myesterday)
            # mtoday = mtoday.strftime("%y%m%d")
            _mdate = (time.strftime('%y%m%d'))
            _mtype = localpath + '*' + myesterday + '*.zip'

            #print(_mtype)
            listFiles = glob.glob(_mtype)
            if listFiles:
                #listFiles.sort()
                modDate = time.strftime('%y%m%d', time.localtime(os.path.getmtime(listFiles[0])))
            #print(modDate)

            _mprocess = 0
            excDigit = 1
            recentFiles = []
            for filename in fnames:
                mfilename = os.path.basename(filename)
                aspath = localpath + mfilename
                modDateF = time.strftime('%y%m%d', time.localtime(os.path.getmtime(aspath)))

                if modDate == modDateF:
                    recentFiles.append(aspath)

            # logging
            logFileName = myesterday + '_logs' + '.log'  # log file
            # if os.path.exists(logFileName):
            #     os.remove(logFileName)  # if log file exists, delete to avoid duplicate info
            logging.basicConfig(filename=logFileName, level=logging.DEBUG, format='%(asctime)s %(message)s',
                                datefmt='%d/%m/%Y %I:%M:%S %p')

            for file in recentFiles:
                mfilename = os.path.basename(file)
                aspath = localpath + mfilename
                #print(mfilename)
                #print(time.strftime('%y%m%d', time.localtime(os.path.getmtime(aspath))))
                #print(mfilename)
                line_list = []
                zfile = zipfile.ZipFile(file)

                if zfile:
                    logging.info('Success Processing File: ' + mfilename)
                    country_code = mfilename[:2]
                    today = date.today()  # - timedelta(days=1)
                    mod_date = datetime.datetime.strptime(modDate, '%y%m%d')
                    mod_date = datetime.datetime.date(mod_date)
                    #print(mod_date)
                    row = (mfilename, today, country_code, mod_date)
                    rowLOG = [row]
                else:
                    logging.warning('Error Processing File: ' + mfilename)

                rowBFH01 = []
                rowBCH = []
                rowBOH = []
                rowBKT06 = []
                rowBKS24 = []
                rowBKS30 = []
                rowBKS31 = []
                rowBKS46 = []
                rowBKS47 = []
                rowBKI61 = []
                rowBKI62 = []
                rowBAR67 = []
                rowBKS39 = []
                rowBKS42 = []
                rowBKS45 = []
                rowBKI63 = []
                rowBAR64 = []
                rowBMD75 = []
                rowBMD76 = []
                rowBAR65 = []
                rowBAR66 = []
                rowBKF81 = []
                rowBKP84 = []
                rowBCC82 = []
                rowBCC83 = []
                rowBCX83 = []
                rowBOT93 = []
                rowBOT94 = []
                rowBCT95 = []
                rowBFT99 = []

                _madg = ''
                _msn = 1

                for finfo in zfile.infolist():
                    ifile = zfile.open(finfo)
                    line_list = ifile.readlines()

                for lines in line_list:
                    _mline = lines.strip()
                    # print(_mline)
                    _mline = _mline.replace('#$%"\|', " ")
                    _mline = _mline.replace("'", " ")

                    #vBKS39_TDNR


                    try:

                        if _mline[:3] == 'BFH' and _mline[11:13] == '01':  # BFH 01 File Header Record
                            vBFH01_SMSG = _mline[0:3]  # Standard Message Identifier SMSG M 3 A 1
                            vBFH01_SQNR = _mline[3:11]  # Sequence Number SQNR M 8 N 4
                            vBFH01_STNQ = _mline[11:13]  # Standard Numeric Qualifier STNQ M 2 N 12
                            vBFH01_BSPI = _mline[13:16]  # BSP Identifier BSPI M 3 AN 14
                            vBFH01_TACN = _mline[16:19]  # Ticketing Airline Code Number TACN M 5 AN 17
                            vBFH01_REVN = _mline[19:22]  # Handbook Revision Number REVN M 3 N 22
                            vBFH01_TPST = _mline[22:26]  # Test/Production Status TPST M 4 AN 25
                            vBFH01_PRDA = _mline[26:32]  # Processing Date PRDA M 6 N 29
                            vBFH01_TIME = _mline[32:36]  # Processing time TIME M 4 N 35
                            vBFH01_ISOC = _mline[36:38]  # ISO Country Code ISOC M 2 A 39
                            vBFH01_FSQN = _mline[38:44]  # File Sequence Number FSQN M 6 N 41
                            vBFH01_RESD = _mline[44:134]  # Reserved Space RESD M 90 AN 47
                            row = (vBFH01_SMSG, vBFH01_SQNR, vBFH01_STNQ, vBFH01_BSPI, vBFH01_TACN, vBFH01_REVN, vBFH01_TPST,
                                   vBFH01_PRDA, vBFH01_TIME, vBFH01_ISOC, vBFH01_FSQN, vBFH01_RESD, mfilename, country_code)
                            # print(row)
                            rowBFH01.append(row)

                        if _mline[:3] == 'BCH' and _mline[11:13] == '02':  # BCH 02 Billing Analysis (Cycle) Header Record
                            vBCH02_SMSG = _mline[0:3]  # Standard Message Identifier SMSG M 3 A 1
                            vBCH02_SQNR = _mline[3:11]  # Sequence Number SQNR M 8 N 4
                            vBCH02_STNQ = _mline[11:13]  # Standard Numeric Qualifier STNQ M 2 N 12
                            vBCH02_PDAI = _mline[13:16]  # Processing Date Identifier PDAI M 3 AN 14
                            vBCH02_PCYC = _mline[16:17]  # Processing Cycle Identifier PCYC M 1 N 17
                            vBCH02_BAED = _mline[17:23]  # Billing Analysis Ending Date BAED M 6 N 18
                            vBCH02_DYRI = _mline[23:24]  # Dynamic Run Identifier DYRI M 1 AN 24
                            vBCH02_HRED = _mline[24:30]  # HOT Reporting End Date M 6 25
                            vBCH02_RESD = _mline[30:136]  # Reserved Space RESD M 112 AN 25
                            row = (vBCH02_SMSG, vBCH02_SQNR, vBCH02_STNQ, vBCH02_PDAI, vBCH02_PCYC, vBCH02_BAED, vBCH02_DYRI,vBCH02_HRED, vBCH02_RESD, mfilename, country_code)
                            rowBCH.append(row)
                        if _mline[:3] == 'BOH' and _mline[11:13] == '03':  # BOH 03 (Reporting Agent) Office Header Record
                            vBOH03_SMSG = _mline[0:3]  # Standard Message Identifier SMSG M 3 A 1
                            vBOH03_SQNR = _mline[3:11]  # Sequence Number SQNR M 8 N 4
                            vBOH03_STNQ = _mline[11:13]  # Standard Numeric Qualifier STNQ M 2 N 12
                            vBOH03_AGTN = _mline[13:21]  # Agent Numeric Code AGTN M 8 N 14
                            vBOH03_RMED = _mline[21:27]  # Remittance Period Ending Date RMED M 6 N 22
                            vBOH03_CUTP = _mline[27:30]  # Currency Type CUTP M 3 AN 28
                            vBOH03_MLOC = _mline[30:33]  # Multi-Location Identifier MLOC C 3 AN 31
                            vBOH03_RESD = _mline[33:105]  # Reserved Space RESD M 79 AN 34
                            row = (vBOH03_SMSG, vBOH03_SQNR, vBOH03_STNQ, vBOH03_AGTN, vBOH03_RMED, vBOH03_CUTP, vBOH03_MLOC,
                                   vBOH03_RESD, mfilename, country_code)
                            rowBOH.append(row)

                        if _mline[:3] == 'BKT' and _mline[11:13] == '06':  # BMD75 Electronic Miscellaneous Document Coupon
                            # Detail Record
                            vBKT06_SMSG = _mline[
                                          0:3].strip()  # Standard Message Identifier SMSG M 3 A 1
                            vBKT06_SQNR = _mline[3:11]  # Sequence Number SQNR M 8 N 4
                            vBKT06_STNQ = _mline[11:13]  # Standard Numeric Qualifier STNQ M 2 N 12
                            vBKT06_TRNN = _mline[13:19]  # Transaction Number TRNN M 6 N 14                      12
                            vBKT06_NRID = _mline[
                                          19:21].strip()  # Net Reporting Indicator NRID C 2 AN 20
                            vBKT06_TREC = _mline[21:24]  # Transaction Record Counter TREC M 3 N 22
                            vBKT06_TACN = _mline[
                                          24:27].strip()  # Ticketing Airline Code Number TACN M 3 AN 25          6
                            vBKT06_CARF = _mline[
                                          27:37].strip()  # Commercial Agreement Reference CARF C 10 AN 28
                            vBKT06_CSTF = _mline[
                                          37:64].strip()  # Customer File Reference CSTF C 27 AN 38
                            vBKT06_RPSI = _mline[
                                          64:68].strip()  # Reporting System Identifier RPSI M 4 AN 65
                            vBKT06_ESAC = _mline[
                                          68:82].strip()  # Settlement Authorisation Code ESAC C 14 AN 69
                            vbKT06_DISI = _mline[
                                          82:83].strip()  # Data Input Status Indicator DISI C 1 AN 83
                            vBKT06_NRMI = _mline[
                                          83:84].strip()  # Net Reporting Method Indicator NRMI C 1 AN 84
                            vBKT06_NRCT = _mline[
                                          84:85].strip()  # Net Reporting Calculation Type NRCT C 1 AN 85
                            vBKT06_AREI = _mline[
                                          85:86].strip()  # Automated Repricing Engine AREI C 1 AN 86
                            vBKT06_RESD = _mline[
                                          86:137].strip()  # Reserved Space RESD M 50 AN 87
                            row = (vBKT06_SMSG, vBKT06_SQNR, vBKT06_STNQ, vBKT06_TRNN, vBKT06_NRID, vBKT06_TREC, vBKT06_TACN,
                                   vBKT06_CARF, vBKT06_CSTF, vBKT06_RPSI, vBKT06_ESAC, vbKT06_DISI, vBKT06_NRMI, vBKT06_NRCT,
                                   vBKT06_AREI, vBKT06_RESD, mfilename, country_code)
                            rowBKT06.append(row)

                        if _mline[:3] == 'BKS' and _mline[11:13] == '39':  # BKS 39 Commission Record
                            vBKS39_SMSG = _mline[0:3]  # Standard Message Identifier SMSG M 3 A 1
                            vBKS39_SQNR = _mline[3:11]  # Sequence Number SQNR M 8 N 4
                            vBKS39_STNQ = _mline[11:13]  # Standard Numeric Qualifier STNQ M 2 N 12
                            vBKS39_DAIS = _mline[13:19]  # Date of Issue DAIS M 6 N 14
                            vBKS39_TRNN = _mline[19:25]  # Transaction Number TRNN M 6 N 20
                            vBKS39_TDNR = _mline[25:39].strip()  # Ticket/Document Number TDNR M 15 AN 26
                            vBKS39_CDGT = _mline[39:40]  # Check-Digit CDGT M 1 N 40
                            vBKS39_STAT = _mline[40:43]  # Statistical Code STAT C 3 AN 42
                            vBKS39_COTP = _mline[43:49]  # Commission Type COTP C 6 AN 45
                            vBKS39_CORT = _mline[49:54]  # Commission Rate CORT M 5 N 51
                            vBKS39_COAM = _mline[54:64]  # Commission Amount COAM M 11 N 56
                            vBKS39_SPTP = _mline[65:71]  # Supplementary Type SPTP C 6 AN 67
                            vBKS39_SPRT = _mline[71:76]  # Supplementary Rate SPRT C 5 N 73
                            vBKS39_SPAM = _mline[76:86]  # Supplementary Amount SPAM C 11 N 78
                            vBKS39_EFRT = _mline[87:92]  # Effective Commission Rate EFRT M 5 N 89
                            vBKS39_EFCO = _mline[92:102]  # Effective Commission Amount EFCO M 11 N 94
                            vBKS39_APBC = _mline[103:113]  # Amount Paid by Customer APBC C 11 N 105
                            vBKS39_RDII = _mline[114:115]  # Routing Domestic/International Indicator
                            vBKS39_CCAI = _mline[115:116]  # Commission Control Adjustment Indicator
                            vBKS39_RESD = _mline[116:132]  # Reserved Space RESD M 3 AN 134
                            vBKS39_CUTP = _mline[132:136]  # Currency Type CUTP M 4 AN 130
                            row = (vBKS39_SMSG, vBKS39_SQNR, vBKS39_STNQ, vBKS39_DAIS, vBKS39_TRNN, vBKS39_TDNR, vBKS39_CDGT,
                                   vBKS39_STAT, vBKS39_COTP, vBKS39_CORT, vBKS39_COAM, vBKS39_SPTP, vBKS39_SPRT, vBKS39_SPAM,
                                   vBKS39_EFRT, vBKS39_EFCO, vBKS39_APBC, vBKS39_RDII, vBKS39_CCAI, vBKS39_RESD, vBKS39_CUTP,
                                   mfilename, country_code)
                            rowBKS39.append(row)

                        if _mline[:3] == 'BKI' and _mline[11:13] == '63':  # BKI 63 Itinerary Data Segment Record
                            vBKI63_SMSG = _mline[0:3]  # Standard Message Identifier SMSG M 3 A 1
                            vBKI63_SQNR = _mline[3:11]  # Sequence Number SQNR M 8 N 4
                            vBKI63_STNQ = _mline[11:13]  # Standard Numeric Qualifier STNQ M 2N 12
                            vBKI63_DAIS = _mline[13:19]  # Date of Issue DAIS M 6N 14
                            vBKI63_TRNN = _mline[19:25]  # Transaction Number TRNN M 6N 20
                            vBKI63_TDNR = _mline[25:39].strip()  # Ticket/Document Number TDNR M 14 AN 26
                            vBKI63_CDGT = _mline[39:40]  # Check-Digit CDGT M 1 N 40
                            vBKI63_SEGI = _mline[40:41]  # Segment Identifier SEGI M 1 N 41
                            vBKI63_STPO = _mline[41:42]  # Stopover Code STPO M 1 A 42
                            vBKI63_NBDA = _mline[42:47]  # Not Valid Before" Date NBDA C 5 AN 43
                            vBKI63_NADA = _mline[47:52]  # Not Valid After" Date NADA C 5 AN 48
                            vBKI63_ORAC = _mline[52:57]  # Origin Airport/ City Code ORAC M 5 A 53
                            vBKI63_DSTC = _mline[57:62]  # Destination Airport/ City Code DSTC M 5 A 58
                            vBKI63_CARR = _mline[62:65]  # Carrier CARR C 3AN 63
                            vBKI63_CABI = _mline[65:66]  # Sold Passenger Cabin CABI C 1 AN 66
                            vBKI63_FTNR = _mline[66:71]  # Flight Number FTNR C 5 AN 67
                            vBKI63_RBKD = _mline[71:73]  # Reservation Booking Designator RBKD M 2 AN 72
                            vBKI63_FTDA = _mline[73:80]  # Flight Date FTDA C 7 AN 74
                            vBKI63_FTDT = _mline[80:85]  # Flight Departure Time FTDT C 5 AN 81
                            vBKI63_FBST = _mline[85:87]  # Flight Booking Status FBST C 2 A 86
                            vBKI63_FBAL = _mline[87:90]  # Free Baggage Allowance FBAL C 3 AN 88
                            vBKI63_FBTD = _mline[90:105]  # Fare Basis/Ticket Designator FBTD M 15 AN 91
                            vBKI63_FFRF = _mline[105:125]  # Frequent Flyer Reference FFRF C 20 AN 106
                            vBKI63_FCPT = _mline[125:128]  # Fare Component Priced Passenger Type Code FCPT C 3 AN 126
                            vBKI63_COGI = _mline[128:129]  # Through/Change of Gauge Indicator COGI C 1 AN 129
                            vBKI63_EQCD = _mline[129:132]  # Equipment Code EQCD C 3 AN 130
                            vBKI63_RESD = _mline[132:136]  # Reserved Space RESD M 4 AN 133

                            row = (vBKI63_SMSG, vBKI63_SQNR, vBKI63_STNQ, vBKI63_DAIS, vBKI63_TRNN, vBKI63_TDNR, vBKI63_CDGT,
                                   vBKI63_SEGI, vBKI63_STPO, vBKI63_NBDA, vBKI63_NADA, vBKI63_ORAC, vBKI63_DSTC, vBKI63_CARR,
                                   vBKI63_CABI, vBKI63_FTNR, vBKI63_RBKD, vBKI63_FTDA, vBKI63_FTDT, vBKI63_FBST, vBKI63_FBAL,
                                   vBKI63_FBTD, vBKI63_FFRF, vBKI63_FCPT, vBKI63_COGI, vBKI63_EQCD, vBKI63_RESD, mfilename,
                                   country_code)
                            rowBKI63.append(row)

                        if _mline[:3] == 'BAR' and _mline[11:13] == '64':
                            # print(_mline)
                            vBAR64_SMSG = _mline[0:3]  # Standard Message Identifier SMSG M 3 A 1
                            vBAR64_SQNR = _mline[3:11]  # Sequence Number SQNR M 8 N 4
                            vBAR64_STNQ = _mline[11:13]  # Standard Numeric Qualifier STNQ M 2 N 12
                            vBAR64_DAIS = _mline[13:19]  # Date of Issue DAIS M 6 N 14
                            vBAR64_TRNN = _mline[19:25]  # Transaction Number TRNN M 6 N 20
                            vBAR64_TDNR = _mline[25:39]  # Ticket/Document Number TDNR M 14 AN 26
                            vBAR64_CDGT = _mline[39:40]  # Check-Digit CDGT M 1 N 40
                            vBAR64_FARE = _mline[40:52]  # Fare FARE M 12 AN 41
                            vBAR64_FARE_CUTP = _mline[40:43]  # Fare CUTP
                            vBAR64_TKMI = _mline[52:53]  # Ticketing Mode Indicator TKMI M 1 AN 53
                            vBAR64_EQFR = _mline[53:65]  # Equivalent Fare Paid EQFR C 12 AN 54
                            vBAR64_EQFR_CUTP = _mline[53:56]  # Equivalent Fare Paid CUTP
                            vBAR64_TOTL = _mline[65:77]  # Total TOTL M 12 AN 66
                            vBAR64_TOTL_CUTP = _mline[65:68]  # Equivalent Fare Paid EQFR C 12 AN 54
                            vBAR64_SASI = _mline[77:81]  # Servicing Airline/System Provider Identifier SASI M 4 AN 78
                            vBAR64_FCMI = _mline[81:82]  # Fare Calculation Mode Indicator FCMI C 1 AN 82
                            vBAR64_BAID = _mline[82:88]  # Booking Agent Identification BAID C 6 AN 83
                            vBAR64_BEOT = _mline[88:89]  # Booking Entity Outlet Type BEOT C 1 AN 89
                            vBAR64_FCPI = _mline[89:90]  # Fare Calculation Pricing Indicator FCPI C 1 AN 90
                            vBAR64_AENT = _mline[90:98]  # Airline Issuing Agent AENT C 8 AN 91
                            vBAR64_RESD = _mline[98:136]  # Reserved Space RESD M 38 AN 99
                            # vBAR64_FARE = (''.join(c for c in _mline[41:52].strip()[3:] if c in digits)).strip()
                            # digit = _mline[76:77]
                            # print(digit)

                            vBAR64_FARE = (''.join(c for c in _mline[40:52].strip()[3:] if c in digits)).strip()
                            if len(vBAR64_FARE) > 0:
                                # print(_mdecimals,_mfare,float(_mfare)/100)
                                if _mline[40:52].find(".") > 0:
                                    vBAR64_FARE_AMT = float(vBAR64_FARE) / 100
                                else:
                                    vBAR64_FARE_AMT = float(vBAR64_FARE)

                            else:
                                vBAR64_FARE_AMT = 0

                            vBAR64_EQFR = _mline[53:65].strip()[0:3]
                            vBAR64_EQFR = (''.join(c for c in _mline[53:65].strip()[3:] if c in digits)).strip()
                            if len(vBAR64_EQFR) > 0:
                                if _mline[53:65].find(".") > 0:
                                    vBAR64_EQFR_AMT = float(vBAR64_EQFR) / 100
                                    # print(_mline[53:65].find("."))
                                else:
                                    vBAR64_EQFR_AMT = float(vBAR64_EQFR)
                            else:
                                vBAR64_EQFR_AMT = 0

                            vBAR64_TOTL = (''.join(c for c in _mline[65:77].strip()[3:] if c in digits)).strip()
                            # print(vBAR64_TOTL)
                            # print(_mline[65:77].find("."))
                            if len(vBAR64_TOTL) > 0:
                                if _mline[65:77].find(".") > 0:
                                    vBAR64_TOTL_AMT = float(vBAR64_TOTL) / 100
                                    # print("yes")
                                else:
                                    vBAR64_TOTL_AMT = float(vBAR64_TOTL)
                                    # print("no")
                            else:
                                vBAR64_TOTL_AMT = 0

                            row = (vBAR64_SMSG, vBAR64_SQNR, vBAR64_STNQ, vBAR64_DAIS, vBAR64_TRNN, vBAR64_TDNR,
                                   vBAR64_CDGT, _mline[40:52], vBAR64_FARE_CUTP, vBAR64_FARE_AMT, vBAR64_TKMI,
                                   _mline[53:65], vBAR64_EQFR_CUTP, vBAR64_EQFR_AMT,
                                   _mline[65:77], vBAR64_TOTL_CUTP, vBAR64_TOTL_AMT, vBAR64_SASI, vBAR64_FCMI, vBAR64_BAID,
                                   vBAR64_BEOT, vBAR64_FCPI, vBAR64_AENT, vBAR64_RESD,
                                   mfilename, country_code)
                            # print(row)
                            rowBAR64.append(row)

                        if _mline[:3] == 'BMD' and _mline[11:13] == '75':  # BMD75 Electronic Miscellaneous Document Coupon
                            # Detail Record
                            vBMD75_SMSG = _mline[
                                          0:3].strip()  # Standard Message Identifier SMSG M 3 A 1
                            vBMD75_SQNR = _mline[
                                          3:11]  # Sequence Number SQNR M 8 N 4
                            vBMD75_STNQ = _mline[11:13]  # Standard Numeric Qualifier STNQ M 2 N 12
                            vBMD75_DAIS = _mline[13:19]  # Date of Issue DAIS M 6 N 14
                            vBMD75_TRNN = _mline[19:25]  # Transaction Number TRNN M 6 N 20
                            vBMD75_TDNR = _mline[25:39].strip()  # Ticket/Document Number TDNR M 14 AN 26
                            vBMD75_CDGT = _mline[39:40]  # Check-Digit CDGT M 1 N 40
                            vBMD75_EMCP = _mline[40:41]  # EMD Coupon Number EMCP M 1 N 41
                            vBMD75_EMCV = _mline[41:51]  # EMD Coupon Value EMCV C 11 N 42
                            vBMD75_EMRT = _mline[52:66]  # EMD Related Ticket/Document EMRT C 14 AN 53
                            vBMD75_EMRC = _mline[66:67]  # EMD Related Coupon Number EMRC C 1 N 67
                            vBMD75_EMST = _mline[67:68]  # EMD Service Type EMST C 1 AN 68
                            vBMD75_EMSC = _mline[68:71]  # EMD Reason for Issuance Sub EMSC M 3 AN 69
                            vBMD75_EMOC = _mline[71:74]  # EMD Fee Owner Airline EMOC C 3 AN 72
                            vBMD75_XBOA = _mline[74:75]  # EMD Excess Baggage Over XBOA C 1 AN 75
                            vBMD75_XBCT = _mline[75:78]  # EMD Excess Baggage Currency XBCT C 3 A 76
                            vBMD75_XBRU = _mline[78:90]  # EMD Excess Baggage Rate per XBRU C 12 AN 79
                            vBMD75_XBNE = _mline[90:102]  # EMD Excess Baggage Total XBNE C 12 AN 91
                            vBMD75_EMCI = _mline[102:103]  # EMD Consumed at Issuance EMCI C 1 AN 103
                            vBMD75_EMNS = _mline[103:106]  # EMD Number of Services EMNS M 3 N 104
                            vBMD75_EMCR = _mline[106:109]  # EMD Operating Carrier EMCR C 3 AN 107
                            vBMD75_EMAG = _mline[109:112]  # EMD Attribute Group EMAG C 3 AN 110
                            vBMD75_EMSG = _mline[112:115]  # EMD Attribute Sub-Group EMSG C 3 AN 113
                            vBMD75_EMIC = _mline[115:116]  # EMD Industry Carrier Indicator EMIC C 1 AN 116
                            vBMD75_RESD = _mline[116:132]  # Reserved Space RESD M 16 AN 117
                            vBMD75_CUTP = _mline[132:137]  # Currency Type CUTP M 4 AN 133
                            row = (vBMD75_SMSG, vBMD75_SQNR, vBMD75_STNQ, vBMD75_DAIS, vBMD75_TRNN, vBMD75_TDNR, vBMD75_CDGT,
                                   vBMD75_EMCP, vBMD75_EMCV, vBMD75_EMRT, vBMD75_EMRC, vBMD75_EMST, vBMD75_EMSC, vBMD75_EMOC,
                                   vBMD75_XBOA, vBMD75_XBCT, vBMD75_XBRU, vBMD75_XBNE, vBMD75_EMCI, vBMD75_EMNS, vBMD75_EMCR,
                                   vBMD75_EMAG, vBMD75_EMSG, vBMD75_EMIC, vBMD75_RESD, vBMD75_CUTP, mfilename, country_code)
                            rowBMD75.append(row)

                        if _mline[:3] == 'BKF' and _mline[11:13] == '81':  # BKF 81 Record Layout
                            vBKF81_SMSG = _mline[0:3]  # Standard Message Identifier SMSG M 3A 1
                            vBKF81_SQNR = _mline[3:11]  # Sequence Number SQNR M 8 N 4
                            vBKF81_STNQ = _mline[11:13]  # Standard Numeric Qualifier STNQ M 2N 12
                            vBKF81_DAIS = _mline[13:19]  # Date of Issue DAIS M 6N 14
                            vBKF81_TRNN = _mline[19:25]  # Transaction Number TRNN M 6N 20
                            vBKF81_TDNR = _mline[25:39]  # Ticket/Document Number TDNR M 14 AN 26
                            vBKF81_CDGT = _mline[39:40]  # Check-Digit CDGT M 1N 40
                            vBKF81_FRCS = _mline[40:41]  # Fare Calculation Sequence Number FRCS C 1 N 41
                            vBKF81_FRCA = _mline[41:128]  # Fare Calculation Area FRCA C 87 AN 42
                            vBKF81_RESD = _mline[128:136]  # Reserved Space RESD M 8 AN 129
                            row = (vBKF81_SMSG, vBKF81_SQNR, vBKF81_STNQ, vBKF81_DAIS, vBKF81_TRNN, vBKF81_TDNR, vBKF81_CDGT,
                                   vBKF81_FRCS, vBKF81_FRCA, vBKF81_RESD, mfilename, country_code)
                            rowBKF81.append(row)

                        if _mline[:3] == 'BKP' and _mline[11:13] == '84':  # BKP 84 Form of Payment Record
                            _mdecimals = int(_mline[135:136])
                            _mdivisor = '1' + ("0" * int(_mdecimals))

                            vBKP84_SMSG = _mline[0:3]  # Standard Message Identifier SMSG M 3 A 1
                            vBKP84_SQNR = _mline[3:11]  # Sequence Number SQNR M 8 N 4
                            vBKP84_STNQ = _mline[11:13]  # Standard Numeric Qualifier STNQ M 2 N 12
                            vBKP84_DAIS = _mline[13:19]  # Date of Issue DAIS M 6 N 14
                            vBKP84_TRNN = _mline[19:25]  # Transaction Number TRNN M 6 N 20
                            vBKP84_FPTP = _mline[25:35]  # Form of Payment Type FPTP M 10 AN 26

                            # Form of Payment Amount FPAM M 11 N 36
                            _msn1, _msymb1, _madg1 = add_check(_msn, _mline[35:46].strip()[-1], _madg)
                            vBKP84_FPAM = (float(_mline[35:46].strip()[0:-1] + _madg1) / float(_mdivisor)) * _msn1

                            vBKP84_FPAC = _mline[46:65]  # Form of Payment Account Number FPAC C 19 AN 47
                            vBKP84_EXDA = _mline[65:69]  # Expiry Date EXDA C 4 AN 66
                            vBKP84_EXPC = _mline[69:71]  # Extended Payment Code EXPC C 2 AN 70
                            vBKP84_APLC = _mline[71:77]  # Approval Code APLC C 6 AN 72
                            vBKP84_INVN = _mline[77:91]  # Invoice Number INVN C 14 AN 78
                            vBKP84_INVD = _mline[91:97]  # Invoice Date INVD C 6 N 92

                            # Remittance Amount REMT M 11 N 98
                            _msn1, _msymb1, _madg1 = add_check(_msn, _mline[97:108].strip()[-1], _madg)
                            vBKP84_REMT = (float(_mline[97:108].strip()[0:-1] + _madg1) / float(_mdivisor)) * _msn1

                            vBKP84_CVVR = _mline[108:109]  # Card Verification Value Result CVVR C 1 AN 109
                            vBKP84_RESD = _mline[109:132]  # Currency Type CUTP C 23 AN 110
                            vBKP84_CUTP = _mline[132:136]  # Reserved Space RESD M 4 AN 133
                            row = (vBKP84_SMSG, vBKP84_SQNR, vBKP84_STNQ, vBKP84_DAIS, vBKP84_TRNN, vBKP84_FPTP, vBKP84_FPAM,
                                   vBKP84_FPAC, vBKP84_EXDA, vBKP84_EXPC, vBKP84_APLC, vBKP84_INVN, vBKP84_INVD, vBKP84_REMT,
                                   vBKP84_CVVR, vBKP84_RESD, vBKP84_CUTP, mfilename, country_code)
                            # print(vBKP84_REMT)
                            rowBKP84.append(row)

                        if _mline[:3] == 'BCC' and _mline[11:13] == '82':  # BCC82 Additional Card Information Record
                            vBCC82_SMSG = _mline[0:3]  # Standard Message Identifier SMSG M 3 A 1
                            vBCC82_SQNR = _mline[3:11]  # Sequence Number SQNR M 8 N 4
                            vBCC82_STNQ = _mline[11:13]  # Standard Numeric Qualifier STNQ M 2 N 12
                            vBCC82_DAIS = _mline[13:19]  # Date of Issue DAIS M 6 N 14
                            vBCC82_TRNN = _mline[19:25]  # Transaction Number TRNN M 6 N 20
                            vBCC82_FPTP = _mline[25:35]  # Form of Payment Type FPTP M 10 AN 26
                            vBCC82_FPTI = _mline[35:60]  # Form of Payment Transaction FPTI M 25 AN 36
                            vBCC82_RESD = _mline[60:136]  # Reserved Space RESD M 76 AN 61
                            row = (vBCC82_SMSG, vBCC82_SQNR, vBCC82_STNQ, vBCC82_DAIS, vBCC82_TRNN, vBCC82_FPTP, vBCC82_FPTI,
                                   vBCC82_RESD, mfilename, country_code)
                            # print(row)
                            rowBCC82.append(row)

                        if _mline[:3] == 'BCC' and _mline[11:13] == '83':  # BFH 01 File Header Record
                            vBCC83_SMSG = _mline[0:3]  # Standard Message Identifier SMSG M 3 A 1
                            vBCC83_SQNR = _mline[3:11]  # Sequence Number SQNR M 8 N 4
                            vBCC83_STNQ = _mline[11:13]  # Standard Numeric Qualifier STNQ M 2 N 12
                            vBCC83_DAIS = _mline[13:19]  # Date of Issue DAIS M 6 N 14
                            vBCC83_TRNN = _mline[19:25]  # Transaction Number TRNN M 6 N 20
                            vBCC83_FPTP = _mline[25:35]  # Form of Payment Type FPTP M 10 AN 26
                            vBCC83_CASN = _mline[35:37]  # Card Authentication Sequence CASN M 2 N 36
                            vBCC83_CATI = _mline[37:136]  # Card Authentication Information CATI M 99 AN 38
                            row = (vBCC83_SMSG, vBCC83_SQNR, vBCC83_STNQ, vBCC83_DAIS, vBCC83_TRNN, vBCC83_FPTP, vBCC83_CASN,
                                   vBCC83_CATI, mfilename, country_code)
                            print(row)
                            rowBCC83.append(row)

                        if _mline[:3] == 'BCX' and _mline[11:13] == '83':  # BCX83 3DS Card Authentication Information Record
                            vBCX83_SMSG = _mline[0:3]  # Standard Message Identifier SMSG M 3 A 1
                            vBCX83_SQNR = _mline[3:11]  # Sequence Number SQNR M 8 N 4
                            vBCX83_STNQ = _mline[11:13]  # Standard Numeric Qualifier STNQ M 2 N 12
                            vBCX83_DAIS = _mline[13:19]  # Date of Issue DAIS M 6 N 14
                            vBCX83_TRNN = _mline[19:25]  # Transaction Number TRNN M 6 N 20
                            vBCX83_FPTP = _mline[25:35]  # Form of Payment Type FPTP M 10 AN 26
                            vBCX83_CASN = _mline[35:37]  # Card Authentication Sequence NumberCASN M 2 N 36
                            vBCX83_TDSD = _mline[37:136]  # 3D Secure Card Authentication TDSD M 99 AN 38
                            row = (vBCX83_SMSG, vBCX83_SQNR, vBCX83_STNQ, vBCX83_DAIS, vBCX83_TRNN, vBCX83_FPTP, vBCX83_CASN,
                                   vBCX83_TDSD,mfilename,country_code)
                            # print(row)
                            rowBCX83.append(row)

                        if _mline[:3] == 'BOT' and _mline[11:13] == '93':  # BOT93 Office Subtotals per Transaction Code and
                            # Currency Type Record
                            _mdecimals = int(_mline[135:136])
                            _mdivisor = '1' + ("0" * int(_mdecimals))

                            vBOT93_SMSG = _mline[0:3]  # Standard Message Identifier SMSG M 3 A 1
                            vBOT93_SQNR = _mline[3:11]  # Sequence Number SQNR M 8 N 4
                            vBOT93_STNQ = _mline[11:13]  # Standard Numeric Qualifier STNQ M 2 N 12
                            vBOT93_AGTN = _mline[13:21]  # Agent Numeric Code AGTN M 8 N 14
                            vBOT93_RMED = _mline[21:27]  # Remittance Period Ending Date RMED M 6 N 22

                            # Gross Value Amount GROS M 15 N 28
                            _msn1, _msymb1, _madg1 = add_check(_msn, _mline[27:42].strip()[-1], _madg)
                            vBOT93_GROS = (float(_mline[27:42].strip()[0:-1] + _madg1) / float(_mdivisor)) * _msn1

                            # Total Remittance Amount TREM M 15 N 43
                            _msn1, _msymb1, _madg1 = add_check(_msn, _mline[42:56].strip()[-1], _madg)
                            vBOT93_TREM = (float(_mline[42:56].strip()[0:-1] + _madg1) / float(_mdivisor)) * _msn1

                            # Total Commission Value Amount TCOM M 15 N 58
                            _msn1, _msymb1, _madg1 = add_check(_msn, _mline[57:71].strip()[-1], _madg)
                            vBOT93_TCOM = (float(_mline[57:71].strip()[0:-1] + _madg1) / float(_mdivisor)) * _msn1

                            # Total Tax/Miscellaneous Fee TTMF M 15 N 73
                            _msn1, _msymb1, _madg1 = add_check(_msn, _mline[72:86].strip()[-1], _madg)
                            vBOT93_TTMF = (float(_mline[72:86].strip()[0:-1] + _madg1) / float(_mdivisor)) * _msn1

                            vBOT93_TRNC = _mline[87:91]  # Transaction Code TRNC M 4 AN 88

                            # Total Tax on Commission Amount TTCA M 15 N 92
                            _msn1, _msymb1, _madg1 = add_check(_msn, _mline[91:106].strip()[-1], _madg)
                            vBOT93_TTCA = (float(_mline[91:106].strip()[0:-1] + _madg1) / float(_mdivisor)) * _msn1

                            vBOT93_RESD = _mline[106:132]  # Reserved Space RESD M 26 AN 107
                            vBOT93_CUTP = _mline[132:136]  # Currency Type CUTP M 4 AN 133
                            row = (vBOT93_SMSG, vBOT93_SQNR, vBOT93_STNQ, vBOT93_AGTN, vBOT93_RMED, vBOT93_GROS, vBOT93_TREM,
                                   vBOT93_TCOM, vBOT93_TTMF, vBOT93_TRNC, vBOT93_TTCA, vBOT93_RESD, vBOT93_CUTP,vBKS24_TDNR,mfilename,country_code)
                            # print(row)
                            rowBOT93.append(row)

                        if _mline[:3] == 'BOT' and _mline[11:13] == '94':  # BOT94 Office Totals per Currency Type Record
                            _mdecimals = int(_mline[135:136])
                            _mdivisor = '1' + ("0" * int(_mdecimals))

                            vBOT94_SMSG = _mline[0:3]  # Standard Message Identifier SMSG M 3 A 1
                            vBOT94_SQNR = _mline[3:11]  # Sequence Number SQNR M 8 N 4
                            vBOT94_STNQ = _mline[11:13]  # Standard Numeric Qualifier STNQ M 2 N 12
                            vBOT94_AGTN = _mline[13:21]  # Agent Numeric Code AGTN M 8 N 14
                            vBOT94_RMED = _mline[21:27]  # Remittance Period Ending Date RMED M 6 N 22

                            # Gross Value Amount GROS M 15 N 28
                            _msn1, _msymb1, _madg1 = add_check(_msn, _mline[27:42].strip()[-1], _madg)
                            vBOT94_GROS = (float(_mline[27:42].strip()[0:-1] + _madg1) / float(_mdivisor)) * _msn1

                            # Total Remittance Amount TREM M 15 N 43
                            _msn1, _msymb1, _madg1 = add_check(_msn, _mline[42:57].strip()[-1], _madg)
                            vBOT94_TREM = (float(_mline[42:57].strip()[0:-1] + _madg1) / float(_mdivisor)) * _msn1

                            # Total Commission Value Amount TCOM M 15 N 58
                            _msn1, _msymb1, _madg1 = add_check(_msn, _mline[57:72].strip()[-1], _madg)
                            vBOT94_TCOM = (float(_mline[57:72].strip()[0:-1] + _madg1) / float(_mdivisor)) * _msn1

                            # Total Tax/Miscellaneous Fee TTMF M 15 N 73
                            _msn1, _msymb1, _madg1 = add_check(_msn, _mline[72:87].strip()[-1], _madg)
                            vBOT94_TTMF = (float(_mline[72:87].strip()[0:-1] + _madg1) / float(_mdivisor)) * _msn1

                            # Total Tax on Commission Amount TTCA M 15 N 88
                            _msn1, _msymb1, _madg1 = add_check(_msn, _mline[87:102].strip()[-1], _madg)
                            vBOT94_TTCA = (float(_mline[87:102].strip()[0:-1] + _madg1) / float(_mdivisor)) * _msn1

                            vBOT94_RESD = _mline[102:132]  # Reserved Space RESD M 30 AN 103
                            vBOT94_CUTP = _mline[132:136]  # Currency Type CUTP M 4 AN 133
                            row = (vBOT94_SMSG, vBOT94_SQNR, vBOT94_STNQ, vBOT94_AGTN, vBOT94_RMED, vBOT94_GROS, vBOT94_TREM,
                                   vBOT94_TCOM, vBOT94_TTMF, vBOT94_TTCA, vBOT94_RESD, vBOT94_CUTP,vBKS24_TDNR,mfilename,country_code)
                            # print(row)
                            rowBOT94.append(row)

                        if _mline[:3] == 'BCT' and _mline[11:13] == '95':  # BCT95 Billing Analysis (Cycle) Totals per
                            # Currency Type Record
                            _mdecimals = int(_mline[135:136])
                            _mdivisor = '1' + ("0" * int(_mdecimals))

                            vBCT95_SMSG = _mline[0:3]  # Standard Message Identifier SMSG M 3 A 1
                            vBCT95_SQNR = _mline[3:11]  # Sequence Number SQNR M 8 N 4
                            vBCT95_STNQ = _mline[11:13]  # Standard Numeric Qualifier STNQ M 2 N 12
                            vBCT95_PDAI = _mline[13:16]  # Processing Date Identifier PDAI M 3 AN 14
                            vBCT95_PCYC = _mline[16:17]  # Processing Cycle Identifier PCYC M 1 N 17
                            vBCT95_OFCC = _mline[17:22]  # Office Count OFCC M 5 N 18

                            # Gross Value Amount GROS M 15 N 23
                            _msn1, _msymb1, _madg1 = add_check(_msn, _mline[22:37].strip()[-1], _madg)
                            vBCT95_GROS = (float(_mline[22:37].strip()[0:-1] + _madg1) / float(_mdivisor)) * _msn1

                            # Total Remittance Amount TREM M 15 N 38
                            _msn1, _msymb1, _madg1 = add_check(_msn, _mline[37:52].strip()[-1], _madg)
                            vBCT95_TREM = (float(_mline[37:52].strip()[0:-1] + _madg1) / float(_mdivisor)) * _msn1

                            # Total Commission Value Amount TCOM M 15 N 53
                            _msn1, _msymb1, _madg1 = add_check(_msn, _mline[52:67].strip()[-1], _madg)
                            vBCT95_TCOM = (float(_mline[52:67].strip()[0:-1] + _madg1) / float(_mdivisor)) * _msn1

                            # Total Tax/Miscellaneous Fee TTMF M 15 N 68
                            _msn1, _msymb1, _madg1 = add_check(_msn, _mline[67:82].strip()[-1], _madg)
                            vBCT95_TTMF = (float(_mline[67:82].strip()[0:-1] + _madg1) / float(_mdivisor)) * _msn1

                            # Total Tax on Commission Amount TTCA M 15 N 83
                            _msn1, _msymb1, _madg1 = add_check(_msn, _mline[82:97].strip()[-1], _madg)
                            vBCT95_TTCA = (float(_mline[82:97].strip()[0:-1] + _madg1) / float(_mdivisor)) * _msn1

                            vBCT95_RESD = _mline[97:132]  # Reserved Space RESD M 35 AN 98
                            vBCT95_CUTP = _mline[132:136]  # Currency Type CUTP M 4 AN 133

                            row = (vBCT95_SMSG, vBCT95_SQNR, vBCT95_STNQ, vBCT95_PDAI, vBCT95_PCYC, vBCT95_OFCC, vBCT95_GROS,
                                   vBCT95_TREM, vBCT95_TCOM, vBCT95_TTMF, vBCT95_TTCA, vBCT95_RESD, vBCT95_CUTP,
                                   mfilename,country_code)
                            # print(row)
                            rowBCT95.append(row)

                        if _mline[:3] == 'BFT' and _mline[11:13] == '99':  # BFT99 File Totals per Currency Type Record

                            _mdecimals = int(_mline[135:136])
                            _mdivisor = '1' + ("0" * int(_mdecimals))

                            vBFT99_SMSG = _mline[0:3]  # Standard Message Identifier SMSG M 3 A 1
                            vBFT99_SQNR = _mline[3:11]  # Sequence Number SQNR M 8 N 4
                            vBFT99_STNQ = _mline[11:13]  # Standard Numeric Qualifier STNQ M 2 N 12
                            vBFT99_BSPI = _mline[13:16]  # BSP Identifier BSPI M 3 AN 14
                            vBFT99_OFCC = _mline[16:21]  # Office Count OFCC M 5 N 17

                            # Gross Value Amount GROS M 15 N 22
                            _msn1, _msymb1, _madg1 = add_check(_msn, _mline[21:36].strip()[-1], _madg)
                            vBFT99_GROS = (float(_mline[21:36].strip()[0:-1] + _madg1) / float(_mdivisor)) * _msn1

                            # Total Remittance Amount TREM M 15 N 37
                            _msn1, _msymb1, _madg1 = add_check(_msn, _mline[36:51].strip()[-1], _madg)
                            vBFT99_TREM = (float(_mline[36:51].strip()[0:-1] + _madg1) / float(_mdivisor)) * _msn1

                            # Total Commission Value Amount TCOM M 15 N 52
                            _msn1, _msymb1, _madg1 = add_check(_msn, _mline[51:66].strip()[-1], _madg)
                            vBFT99_TCOM = (float(_mline[51:66].strip()[0:-1] + _madg1) / float(_mdivisor)) * _msn1

                            # Total Tax/Miscellaneous Fee TTMF M 15 N 67
                            _msn1, _msymb1, _madg1 = add_check(_msn, _mline[66:81].strip()[-1], _madg)
                            vBFT99_TTMF = (float(_mline[66:81].strip()[0:-1] + _madg1) / float(_mdivisor)) * _msn1

                            # Total Tax on Commission Amount TTCA M 15 N 82
                            _msn1, _msymb1, _madg1 = add_check(_msn, _mline[81:96].strip()[-1], _madg)
                            vBFT99_TTCA = (float(_mline[81:96].strip()[0:-1] + _madg1) / float(_mdivisor)) * _msn1

                            vBFT99_RESD = _mline[96:132]  # Reserved Space RESD M 36 AN 97
                            vBFT99_CUTP = _mline[132:136]  # Currency Type CUTP M 4 AN 133
                            row = (vBFT99_SMSG, vBFT99_SQNR, vBFT99_STNQ, vBFT99_BSPI, vBFT99_OFCC, vBFT99_GROS,
                                   vBFT99_TREM, vBFT99_TCOM, vBFT99_TTMF, vBFT99_TTCA, vBFT99_RESD, vBFT99_CUTP,
                                   mfilename, country_code)
                            # print(row)
                            rowBFT99.append(row)

                        if _mline[:3] == 'BKS' and _mline[11:13] == '24':
                            vBKS24_SMSG = _mline[0:3]
                            vBKS24_SQNR = _mline[3:11]
                            vBKS24_STNQ = _mline[11:13]
                            vBKS24_DAIS = _mline[13:19]
                            vBKS24_TRNN = _mline[19:25]
                            global vBKS24_TDNR
                            vBKS24_TDNR = _mline[25:39]
                            vBKS24_CDGT = _mline[39:40]
                            vBKS24_CPUI = _mline[40:44]
                            vBKS24_CJCP = _mline[44:47]
                            vBKS24_AGTN = _mline[47:55]
                            vBKS24_RFIC = _mline[55:56]
                            vBKS24_TOUR = _mline[56:71]
                            vBKS24_TRNC = _mline[71:75]
                            vBKS24_TODC = _mline[75:85]
                            vBKS24_PNRR = _mline[85:98]
                            vBKS24_TIIS = _mline[98:102]
                            vBKS24_TACC = _mline[102:107]
                            vBKS24_RESD = _mline[107:136]
                            row = (
                            vBKS24_SMSG, vBKS24_SQNR, vBKS24_STNQ, vBKS24_DAIS, vBKS24_TRNN, vBKS24_TDNR, vBKS24_CDGT,
                            vBKS24_CPUI, vBKS24_CJCP, vBKS24_AGTN, vBKS24_RFIC, vBKS24_TOUR, vBKS24_TRNC, vBKS24_TODC,
                            vBKS24_PNRR, vBKS24_TIIS, vBKS24_TACC, vBKS24_RESD, mfilename, country_code)
                            rowBKS24.append(row)

                        if _mline[:3] == 'BKS' and _mline[11:13] == '30':
                            _mdecimals = int(_mline[135:136])
                            _mdivisor = '1' + ("0" * int(_mdecimals))
                            #print(_mline[135:136])

                            vBKS30_SMSG = _mline[0:3]
                            vBKS30_SQNR = _mline[3:11]
                            vBKS30_STNQ = _mline[11:13]
                            vBKS30_DAIS = _mline[13:19]
                            vBKS30_TRNN = _mline[19:25]
                            vBKS30_TDNR = _mline[25:39]
                            vBKS30_CDGT = _mline[39:40]
                            # vBKS30_COBL = _mline[40:51] #this line
                            # vBKS30_NTFA = _mline[51:62] #this line
                            vBKS30_TMFT1 = _mline[62:70]
                            # vBKS30_TMFA1 = _mline[70:81] #this line
                            vBKS30_TMFT2 = _mline[81:89]
                            # vBKS30_TMFA2 = _mline[89:100] #this line
                            vBKS30_TMFT3 = _mline[100:108]
                            # vBKS30_TMFA3 = _mline[108:119] #this line
                            # vBKS30_TDAM = _mline[119:130] #this line
                            vBKS30_RESD = _mline[130:132]
                            vBKS24_CUTP = _mline[132:136]

                            _msn1, _msymb1, _madg1 = add_check(_msn, _mline[40:51].strip()[-1], _madg)  # cobl
                            vBKS30_COBL = (float(_mline[40:51].strip()[0:-1] + _madg1) / float(_mdivisor)) * _msn1

                            _msn1, _msymb1, _madg1 = add_check(_msn, _mline[51:62].strip()[-1], _madg)  # ntfa
                            vBKS30_NTFA = (float(_mline[51:62].strip()[0:-1] + _madg1) / float(_mdivisor)) * _msn1

                            _msn1, _msymb1, _madg1 = add_check(_msn, _mline[70:81].strip()[-1], _madg)  # tmfa1
                            vBKS30_TMFA1 = (float(_mline[70:81].strip()[0:-1] + _madg1) / float(_mdivisor)) * _msn1

                            _msn1, _msymb1, _madg1 = add_check(_msn, _mline[89:100].strip()[-1], _madg)  # tmfa2
                            vBKS30_TMFA2 = (float(_mline[89:100].strip()[0:-1] + _madg1) / float(_mdivisor)) * _msn1

                            _msn1, _msymb1, _madg1 = add_check(_msn, _mline[108:119].strip()[-1], _madg)  # tmfa3
                            vBKS30_TMFA3 = (float(_mline[108:119].strip()[0:-1] + _madg1) / float(_mdivisor)) * _msn1

                            _msn1, _msymb1, _madg1 = add_check(_msn, _mline[119:130].strip()[-1], _madg)  # tdam
                            vBKS30_TDAM = (float(_mline[119:130].strip()[0:-1] + _madg1) / float(_mdivisor)) * _msn1

                            row = (
                            vBKS30_SMSG, vBKS30_SQNR, vBKS30_STNQ, vBKS30_DAIS, vBKS30_TRNN, vBKS30_TDNR, vBKS30_CDGT,
                            vBKS30_COBL, vBKS30_NTFA, vBKS30_TMFT1, vBKS30_TMFA1, vBKS30_TMFT2, vBKS30_TMFA2,
                            vBKS30_TMFT3, vBKS30_TMFA3, vBKS30_TDAM, vBKS30_RESD, vBKS24_CUTP, mfilename, country_code)
                            # print(row)
                            rowBKS30.append(row)

                        if _mline[:3] == 'BKS' and _mline[11:13] == '42':
                            _mdivisor = '1' + ("0" * int(_mdecimals))
                            _mdecimals = int(_mline[135:136])

                            vBKS42_SMSG = _mline[0:3]
                            vBKS42_SQNR = _mline[3:11]
                            vBKS42_STNQ = _mline[11:13]
                            vBKS42_DAIS = _mline[13:19]
                            vBKS42_TRNN = _mline[19:25]
                            vBKS42_TDNR = _mline[25:39]
                            vBKS42_CDGT = _mline[39:40]
                            vBKS42_TCTP1 = _mline[40:46]
                            vBKS42_TOCA1 = _mline[46:57]  # this line
                            vBKS42_TCTP2 = _mline[57:63]
                            vBKS24_TOCA2 = _mline[63:74]  # this line
                            vBKS42_TCTP3 = _mline[74:80]
                            vBKS42_TOCA3 = _mline[80:91]  # this line
                            vBKS42_TCTP4 = _mline[91:97]
                            vBKS42_TOCA4 = _mline[97:108]  # this line
                            vBKS42_RESD = _mline[108:132]
                            vBKS42_CUTP = _mline[132:136]

                            _msn1, _msymb1, _madg1 = add_check(_msn, _mline[46:57].strip()[-1], _madg)
                            vBKS42_TOCA1 = (float(_mline[46:57].strip()[0:-1] + _madg1) / float(_mdivisor)) * _msn1
                            _msn1, _msymb1, _madg1 = add_check(_msn, _mline[63:74].strip()[-1], _madg)
                            vBKS24_TOCA2 = (float(_mline[63:74].strip()[0:-1] + _madg1) / float(_mdivisor)) * _msn1
                            _msn1, _msymb1, _madg1 = add_check(_msn, _mline[80:91].strip()[-1], _madg)
                            vBKS42_TOCA3 = (float(_mline[80:91].strip()[0:-1] + _madg1) / float(_mdivisor)) * _msn1
                            _msn1, _msymb1, _madg1 = add_check(_msn, _mline[97:108].strip()[-1], _madg)
                            vBKS42_TOCA4 = (float(_mline[97:108].strip()[0:-1] + _madg1) / float(_mdivisor)) * _msn1

                            row = (
                            vBKS42_SMSG, vBKS42_SQNR, vBKS42_STNQ, vBKS42_DAIS, vBKS42_TRNN, vBKS42_TDNR, vBKS42_CDGT,
                            vBKS42_TCTP1, vBKS42_TOCA1, vBKS42_TCTP2, vBKS24_TOCA2, vBKS42_TCTP3, vBKS42_TOCA3,
                            vBKS42_TCTP4, vBKS42_TOCA4, vBKS42_RESD, vBKS42_CUTP, mfilename, country_code)
                            rowBKS42.append(row)
                            # print(row)

                        if _mline[:3] == 'BKS' and _mline[11:13] == '45':
                            vBKS45_SMSG = _mline[0:3]
                            vBKS45_SNQR = _mline[3:11]
                            vBKS45_STNQ = _mline[11:13]
                            vBKS45_RMED = _mline[13:19]
                            vBKS45_TRNN = _mline[19:25]
                            vBKS45_RTDN = _mline[25:39]
                            vBKS45_CDGT = _mline[39:40]
                            vBKS45_WAVR = _mline[40:54]
                            vBKS45_RMIC = _mline[54:59]
                            vBKS45_RCPN = _mline[59:63]
                            vBKS45_DIRD = _mline[63:69]
                            vBKS45_RESD = _mline[69:136]
                            row = (
                            vBKS45_SMSG, vBKS45_SNQR, vBKS45_STNQ, vBKS45_RMED, vBKS45_TRNN, vBKS45_RTDN, vBKS45_CDGT,
                            vBKS45_WAVR, vBKS45_RMIC, vBKS45_RCPN, vBKS45_DIRD, vBKS45_RESD, mfilename, country_code)
                            rowBKS45.append(row)

                        if _mline[:3] == 'BAR' and _mline[11:13] == '65':
                            vBAR65_SMSG = _mline[0:3]
                            vBAR65_SQNR = _mline[3:11]
                            vBAR65_STNQ = _mline[11:13]
                            vBAR65_DAIS = _mline[13:19]
                            vBAR65_TRNN = _mline[19:25]
                            vBAR65_TDNR = _mline[25:39]
                            vBAR65_CDGT = _mline[39:40]
                            vBAR65_PXNM = _mline[40:89]
                            vBAR65_PXDA = _mline[89:118]
                            vBAR65_DOBR = _mline[118:125]
                            vBAR65_PXTP = _mline[125:128]
                            vBAR65_RESD = _mline[128:136]
                            row = (
                            vBAR65_SMSG, vBAR65_SQNR, vBAR65_STNQ, vBAR65_DAIS, vBAR65_TRNN, vBAR65_TDNR, vBAR65_CDGT,
                            vBAR65_PXNM, vBAR65_PXDA, vBAR65_DOBR, vBAR65_PXTP, vBAR65_RESD, mfilename, country_code)
                            rowBAR65.append(row)

                        if _mline[:3] == 'BAR' and _mline[11:13] == '66':
                            vBAR66_SMSG = _mline[0:3]
                            vBAR66_SQNR = _mline[3:11]
                            vBAR66_STNQ = _mline[11:13]
                            vBAR66_DAIS = _mline[13:19]
                            vBAR66_TRNN = _mline[19:25]
                            vBAR66_TDNR = _mline[25:39]
                            vBAR66_CDGT = _mline[39:40]
                            vBAR66_FPSN = _mline[40:41]
                            vBAR66_FPIN = _mline[41:91]
                            vBAR66_RESD = _mline[91:136]
                            row = (
                            vBAR66_SMSG, vBAR66_SQNR, vBAR66_STNQ, vBAR66_DAIS, vBAR66_TRNN, vBAR66_TDNR, vBAR66_CDGT,
                            vBAR66_FPSN, vBAR66_FPIN, vBAR66_RESD, mfilename, country_code)
                            rowBAR66.append(row)

                        if _mline[:3] == 'BMD' and _mline[11:13] == '76':
                            vBMD76_SMSG = _mline[0:3]
                            vBMD76_SQNR = _mline[3:11]
                            vBMD76_STNQ = _mline[11:13]
                            vBMD76_DAIS = _mline[13:19]
                            vBMD76_TRNN = _mline[19:25]
                            vBMD76_TDNR = _mline[25:39]
                            vBMD76_CDGT = _mline[39:40]
                            vBMD76_EMCP = _mline[40:41]
                            vBMD76_EMRM = _mline[41:111]
                            vBMD76_RESD = _mline[111:136]
                            row = (
                            vBMD76_SMSG, vBMD76_SQNR, vBMD76_STNQ, vBMD76_DAIS, vBMD76_TRNN, vBMD76_TDNR, vBMD76_CDGT,
                            vBMD76_EMCP, vBMD76_EMRM, vBMD76_RESD, mfilename, country_code)
                            rowBMD76.append(row)

                        if _mline[:3] == 'BKS' and _mline[11:13] == '31':  # no data
                            vBKS31_SMSG = _mline[0:3]  # Standard Message Identifier SMSG M 3 A 1
                            vBKS31_SQNR = _mline[3:11]
                            vBKS31_STNQ = _mline[11:13]
                            vBKS31_DAIS = _mline[13:19]
                            vBKS31_TRNN = _mline[19:25]
                            vBKS31_TDNR = _mline[25:39]
                            vBKS31_CDGT = _mline[39:40]
                            vBKS31_SEGI1 = _mline[40:41]
                            vBKS31_CTAC1 = _mline[41:46]
                            vBKS31_STAC1 = _mline[46:52]
                            vBKS31_CTCD1 = _mline[52:54]
                            vBKS31_CTTP1 = _mline[54:57]
                            vBKS31_CTRA1 = _mline[57:68]
                            vBKS31_CUTX1 = _mline[68:72]
                            vBKS31_CTAA1 = _mline[72:83]
                            vBKS31_SEGI2 = _mline[83:84]
                            vBKS31_CTAC2 = _mline[84:89]
                            vBKS31_STAC2 = _mline[89:95]
                            vBKS31_CTCD2 = _mline[95:97]
                            vBKS31_CTTP2 = _mline[97:100]
                            vBKS31_CTRA2 = _mline[100:111]
                            vBKS31_CUTX2 = _mline[111:115]
                            vBKS31_CTAA2 = _mline[115:126]
                            vBKS31_RESD = _mline[126:132]
                            vBKS31_CUTP = _mline[132:136]
                            row = (
                            vBKS31_SMSG, vBKS31_SQNR, vBKS31_STNQ, vBKS31_DAIS, vBKS31_TRNN, vBKS31_TDNR, vBKS31_CDGT,
                            vBKS31_SEGI1, vBKS31_CTAC1, vBKS31_STAC1, vBKS31_CTCD1, vBKS31_CTTP1, vBKS31_CTRA1,
                            vBKS31_CUTX1, vBKS31_CTAA1, vBKS31_SEGI2, vBKS31_CTAC2, vBKS31_STAC2, vBKS31_CTCD2,
                            vBKS31_CTTP2, vBKS31_CTRA2, vBKS31_CUTX2, vBKS31_CTAA2, vBKS31_RESD, vBKS31_CUTP, mfilename,
                            country_code)
                            rowBKS31.append(row)
                            # print(row)

                        if _mline[:3] == 'BKS' and _mline[11:13] == '46':  # no data
                            vBKS46_SMSG = _mline[0:3]
                            vBKS46_SQNR = _mline[3:11]
                            vBKS46_STNQ = _mline[11:13]
                            vBKS46_DAIS = _mline[13:19]
                            vBKS46_TRNN = _mline[19:25]
                            vBKS46_TDNR = _mline[25:39]
                            vBKS46_CDGT = _mline[39:40]
                            vBKS46_ORIT = _mline[40:54]
                            vBKS46_ORIL = _mline[54:57]
                            vBKS46_ORID = _mline[57:64]
                            vBKS46_ORIA = _mline[64:72]
                            vBKS46_ENRS = _mline[72:121]
                            vBKS46_RESD = _mline[121:136]
                            row = (
                                vBKS46_SMSG, vBKS46_SQNR, vBKS46_STNQ, vBKS46_DAIS, vBKS46_TRNN, vBKS46_TDNR, vBKS46_CDGT,
                                vBKS46_ORIT, vBKS46_ORIL, vBKS46_ORID, vBKS46_ORIA, vBKS46_ENRS, vBKS46_RESD, mfilename,
                                country_code)
                            rowBKS46.append(row)
                            # print(row)

                        if _mline[:3] == 'BKS' and _mline[11:13] == '47':  # no data
                            vBKS47_SMSG = _mline[0:3]
                            vBKS47_SQNR = _mline[3:11]
                            vBKS47_STNQ = _mline[11:13]
                            vBKS47_DAIS = _mline[13:19]
                            vBKS47_TRNN = _mline[19:25]
                            vBKS47_TDNR = _mline[25:39]
                            vBKS47_CDGT = _mline[39:40]
                            vBKS47_NTTP1 = _mline[40:41]
                            vBKS47_NTTC1 = _mline[41:49]
                            vBKS47_NTTA1 = _mline[49:60]
                            vBKS47_NTTP2 = _mline[60:61]
                            vBKS47_NTTC2 = _mline[61:69]
                            vBKS47_NTTA2 = _mline[69:80]
                            vBKS47_NTTP3 = _mline[80:81]
                            vBKS47_NTTC3 = _mline[81:89]
                            vBKS47_NTTA3 = _mline[89:100]
                            vBKS47_NTTP4 = _mline[100:101]
                            vBKS47_NTTC4 = _mline[101:109]
                            vBKS47_NTTA4 = _mline[109:120]
                            vBKS47_RESD = _mline[120:132]
                            vBKS47_CUTP = _mline[132:136]
                            row = (
                                vBKS47_SMSG, vBKS47_SQNR, vBKS47_STNQ, vBKS47_DAIS, vBKS47_TRNN, vBKS47_TDNR, vBKS47_CDGT,
                                vBKS47_NTTP1, vBKS47_NTTC1, vBKS47_NTTA1, vBKS47_NTTP2, vBKS47_NTTC2, vBKS47_NTTA2,
                                vBKS47_NTTP3, vBKS47_NTTC3, vBKS47_NTTA3, vBKS47_NTTP4, vBKS47_NTTC4, vBKS47_NTTA4,
                                vBKS47_RESD, vBKS47_CUTP, mfilename, country_code)
                            rowBKS47.append(row)
                            # print(row)

                        if _mline[0:3] == 'BKI' and _mline[11:13] == '61':  # no data
                            vBKI61_SMSG = _mline[0:3]
                            vBKI61_SQNR = _mline[3:11]
                            vBKI61_STNQ = _mline[11:13]
                            vBKI61_DAIS = _mline[13:19]
                            vBKI61_TRNN = _mline[19:25]
                            vBKI61_TDNR = _mline[25:39]
                            vBKI61_CDGT = _mline[39:40]
                            vBKI61_SEGI = _mline[40:41]
                            vBKI61_UTPC = _mline[41:46]
                            vBKI61_UPDA = _mline[46:53]
                            vBKI61_UPTA = _mline[53:58]
                            vBKI61_UPDD = _mline[58:65]
                            vBKI61_UPTD = _mline[65:70]
                            vBKI61_UPEQ = _mline[70:73]
                            vBKI61_RESD = _mline[73:136]
                            row = (
                                vBKI61_SMSG, vBKI61_SQNR, vBKI61_STNQ, vBKI61_DAIS, vBKI61_TRNN, vBKI61_TDNR, vBKI61_CDGT,
                                vBKI61_SEGI, vBKI61_UTPC, vBKI61_UPDA, vBKI61_UPTA, vBKI61_UPDD, vBKI61_UPTD, vBKI61_UPEQ,
                                vBKI61_RESD, mfilename, country_code)
                            rowBKI61.append(row)
                            # print(row)

                        if _mline[:3] == 'BKI' and _mline[11:13] == '62':  # no data
                            vBKI61_SMSG = _mline[0:3]
                            vBKI62_SQNR = _mline[3:11]
                            vBKI62_STNQ = _mline[11:13]
                            vBKI62_DAIS = _mline[13:19]
                            vBKI62_TRNN = _mline[19:25]
                            vBKI62_TDNR = _mline[25:39]
                            vBKI62_CDGT = _mline[39:40]
                            vBKI62_SEGI = _mline[40:41]
                            vBKI62_ORAC = _mline[41:46]
                            vBKI62_FTDA = _mline[46:53]
                            vBKI62_FTDT = _mline[53:58]
                            vBKI62_FDTE = _mline[58:63]
                            vBKI62_DSTC = _mline[63:68]
                            vBKI62_FTAD = _mline[68:75]
                            vBKI62_FTAT = _mline[75:80]
                            vBKI62_FATE = _mline[80:85]
                            vBKI62_RESD = _mline[85:136]
                            row = (
                                vBKI61_SMSG, vBKI62_SQNR, vBKI62_STNQ, vBKI62_DAIS, vBKI62_TRNN, vBKI62_TDNR, vBKI62_CDGT,
                                vBKI62_SEGI, vBKI62_ORAC, vBKI62_FTDA, vBKI62_FTDT, vBKI62_FDTE, vBKI62_DSTC, vBKI62_FTAD,
                                vBKI62_FTAT, vBKI62_FATE, vBKI62_RESD, mfilename, country_code)
                            rowBKI62.append(row)

                        if _mline[:3] == 'BAR' and _mline[11:13] == '67':
                            vBAR67_SMSG = _mline[0:3]
                            vBAR67_SQNR = _mline[3:11]
                            vBAR67_STNQ = _mline[11:13]
                            vBAR67_DAIS = _mline[13:19]
                            vBAR67_TRNN = _mline[19:25]
                            vBAR67_TDNR = _mline[25:39]
                            vBAR67_CDGT = _mline[39:40]
                            vBAR67_TXSN = _mline[40:42]
                            vBAR67_TXID = _mline[42:46]
                            vBAR67_TXIN = _mline[46:116]
                            vBAR67_RESD = _mline[116:136]
                            row = (
                                vBAR67_SMSG, vBAR67_SQNR, vBAR67_STNQ, vBAR67_DAIS, vBAR67_TRNN, vBAR67_TDNR, vBAR67_CDGT,
                                vBAR67_TXSN, vBAR67_TXID, vBAR67_TXIN, vBAR67_RESD, mfilename, country_code)
                            rowBAR67.append(row)
                            # print(row)

                        #your code

                    except Exception:
                        f = open(logFileName, "a+")
                        f.write("\n" + "ERROR UNABLE TO READ DATA OF: " + mfilename + "\n")
                        f.write(error_handling() + "\n")
                        excDigit = 0
                        #logging.warning("Error reading: "+mfilename+"\n")
                        #logging.warning(error_handling())

                    #print(excDigit)
                    if excDigit == 0:
                        _mprocess = 0
                    else:
                        _mprocess = 1

                if _mprocess == 1:
                    try:
                        cursor.prepare("INSERT INTO DATABASE_NAME.V_BSP_LOG_TABLE(FILENAME,PROCESSING_DATE,"
                                       "COUNTRY_CODE,MOD_DATE) "
                                       "VALUES (:1, :2, :3, :4)")
                        cursor.executemany(None, rowLOG)
                        cursor.prepare(
                            'insert into DATABASE_NAME.V_BSP_BFH01_FILE_HEADER_RECORD(STANDARD_MESSAGE_IDENTIFIER,'
                            'SEQUENCE_NUMBER,STANDARD_NUMERIC_QUALIFIER,BSP_IDENTIFIER,TICKETING_AIRLINE_CODE_NUMBER,'
                            'HANDBOOK_REVISION_NUMBER,TEST_PRODUCTION_STATUS,PROCESSING_DATE,PROCESSING_TIME,'
                            'ISO_COUNTRY_CODE,FILE_SEQUENCE_NUMBER,RESERVED_SPACE,FILENAME, COUNTRY_CODE) '
                            'values(:1, :2, :3, :4, :5, :6, :7, :8, '
                            ':9, :10, :11, :12, :13, :14)')
                        cursor.executemany(None, rowBFH01)
                        cursor.prepare(
                            'insert into DATABASE_NAME.V_BSP_BCH02_BILLING_ANALYSIS(STANDARD_MESSAGE_IDENTIFIER,'
                            'SEQUENCE_NUMBER,STANDARD_NUMERIC_QUALIFIER,PROCESSING_DATE_IDENTIFIER,'
                            'PROCESSING_CYCLE_IDENTIFIER,BILLING_ANALYSIS_ENDING_DATE,DYNAMIC_RUN_IDENTIFIER,'
                            'HOT_REPORTING_END_DATE,RESERVED_SPACE,FILENAME,COUNTRY_CODE)'
                            ' values(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)')
                        cursor.executemany(None, rowBCH)
                        cursor.prepare(
                            'insert into DATABASE_NAME.V_BSP_BOH03_REPORTING_AGENT(STANDARD_MESSAGE_IDENTIFIER,'
                            'SEQUENCE_NUMBER,STANDARD_NUMERIC_QUALIFIER,AGENT_NUMERIC_CODE,REMITTANCE_PERIOD_ENDING_DATE,'
                            'CURRENCY_TYPE,MULTI_LOCATION_IDENTIFIER,RESERVED_SPACE,FILENAME,COUNTRY_CODE) '
                            'values(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)')
                        cursor.executemany(None, rowBOH)
                        cursor.prepare(
                            'insert into DATABASE_NAME.V_BSP_BKT06_TRANSACTION_HEADER(STANDARD_MESSAGE_IDENTIFIER,'
                            'SEQUENCE_NUMBER,STANDARD_NUMERIC_QUALIFIER,TRANSACTION_NUMBER,NET_REPORTING_INDICATOR,'
                            'TRANSACTION_RECORD_COUNTER,TICKETING_AIRLINE_CODE,COMMERCIAL_AGREEMENT_REFERENCE,'
                            'CUSTOMER_FILE_REFERENCE,REPORTING_SYSTEM_IDENTIFIER,SETTLEMENT_AUTHORISATION_CODE,'
                            'DATA_INPUT_STATUS_INDICATOR,NET_REPORTING_METHOD_INDICATOR,NET_REPORTING_CALCULATION_TYPE,'
                            'AUTOMATED_REPRICING_ENG_IND,RESERVED_SPACE,FILENAME,COUNTRY_CODE) '
                            'values(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, '
                            ':12, :13, :14, :15, :16, :17, :18)')
                        cursor.executemany(None, rowBKT06)
                        cursor.prepare(
                            'insert into DATABASE_NAME.V_BSP_BKS39_COMMISSION_RECORD(STANDARD_MESSAGE_IDENTIFIER,'
                            'SEQUENCE_NUMBER,STANDARD_NUMERIC_QUALIFIER,DATE_OF_ISSUE,TRANSACTION_NUMBER,'
                            'TICKET_DOCUMENT_NUMBER,CHECK_DIGIT,STATISTICAL_CODE,COMMISSION_TYPE,COMMISSION_RATE,'
                            'COMMISSION_AMOUNT,SUPPLEMENTARY_TYPE,SUPPLEMENTARY_RATE,SUPPLEMENTARY_AMOUNT,'
                            'EFFECTIVE_COMMISSION_RATE,EFFECTIVE_COMMISSION_AMOUNT,AMOUNT_PAID_BY_CUSTOMER,'
                            'ROUTING_DOMESTIC_INTERNATIONAL,COMMISSION_CONTROL_ADJUSTMENT,RESERVED_SPACE,CURRENCY_TYPE,'
                            'FILENAME,COUNTRY_CODE) '
                            'values(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, '
                            ':20, :21, :22, :23)')
                        cursor.executemany(None, rowBKS39)
                        cursor.prepare(
                            'insert into DATABASE_NAME.V_BSP_BKI63_ITINERARY_DATA(STANDARD_MESSAGE_IDENTIFIER,'
                            'SEQUENCE_NUMBER,STANDARD_NUMERIC_QUALIFIER,DATE_OF_ISSUE,TRANSACTION_NUMBER,'
                            'TICKET_DOCUMENT_NUMBER,CHECK_DIGIT,SEGMENT_IDENTIFIER,STOPOVER_CODE,NOT_VALID_BEFORE_DATE,'
                            'NOT_VALID_AFTER_DATE,ORIGIN_AIRPORT_CITY_CODE,DESTINATION_AIRPORT_CITY_CODE,CARRIER,'
                            'SOLD_PASSENGER_CABIN,FLIGHT_NUMBER,RESERVATION_BOOKING_DESIGNATOR,FLIGHT_DEPARTURE_DATE,'
                            'FLIGHT_DEPARTURE_TIME,FLIGHT_BOOKING_STATUS,BAGGAGE_ALLOWANCE,FARE_BASIS_TICKET_DESIGNATOR,'
                            'FREQUENT_FLYER_REFERENCE,FARE_COMPONENT_PRICED,THROUGH_CHANGE_OF_GAUGE,EQUIPMENT_CODE,'
                            'RESERVED_SPACE,FILENAME,COUNTRY_CODE)  '
                            'values(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16,'
                            ':17, :18, :19, :20, :21, :22, :23, :24, :25, :26, :27, :28, :29)')
                        cursor.executemany(None, rowBKI63)
                        cursor.prepare(
                            'insert into DATABASE_NAME.V_BSP_BAR64_DOCUMENT_AMOUNTS(STANDARD_MESSAGE_IDENTIFIER,'
                            'SEQUENCE_NUMBER,STANDARD_NUMERIC_QUALIFIER,DATE_OF_ISSUE,TRANSACTION_NUMBER,'
                            'TICKET_DOCUMENT_NUMBER,CHECK_DIGIT,FARE,FARE_CUTP,FARE_AMT,TICKETING_MODE_INDICATOR,'
                            'EQUIVALENT_FARE_PAID,EQFR_CUTP,EQFR_AMT,TOTAL,TOTAL_CUTP,TOTAL_AMT,'
                            'SERVICING_AIRLINE_SYSTEM_ID,FARE_CALCULATION_MODE_IND,BOOKING_AGENT_IDENTIFICATION,'
                            'BOOKING_ENTITY_OUTLET_TYPE,FARE_CALCULATION_PRICING,AIRLINE_ISSUING_AGENT,RESERVED_SPACE,'
                            'FILENAME,COUNTRY_CODE)  '
                            'values(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16,:17, :18, :19, '
                            ':20,:21,:22,:23,:24,:25,:26)')
                        cursor.executemany(None, rowBAR64)
                        cursor.prepare(
                            'insert into DATABASE_NAME.V_BSP_BMD75_EMD_DETAILS(STANDARD_MESSAGE_IDENTIFIER,'
                            'SEQUENCE_NUMBER,STANDARD_NUMERIC_QUALIFIER,DATE_OF_ISSUE,TRANSACTION_NUMBER,'
                            'TICKET_DOCUMENT_NUMBER,CHECK_DIGIT,'
                            'EMD_COUPON_NUMBER,EMD_COUPON_VALUE,EMD_RELATED_TICKET,EMD_RELATED_COUPON_NUMBER,'
                            'EMD_SERVICE_TYPE,EMD_REASON_FOR_ISSUANCE,EMD_FEE_OWNER_AIRLINE,EMD_EXCESS_BAGGAGE_OVER,'
                            'EMD_EXCESS_BAGGAGE_CURRENCY,EMD_EXCESS_BAGGAGE_RATE,EMD_EXCESS_BAGGAGE_TOTAL,'
                            'EMD_CONSUMED_AT_ISSUANCE,EMD_NUMBER_OF_SERVICES,EMD_OPERATING_CARRIER,EMD_ATTRIBUTE_GROUP,'
                            'EMD_ATTRIBUTE_SUB_GROUP,EMD_INDUSTRY_CARRIER_INDICATOR,RESERVED_SPACE,CURRENCY_TYPE,'
                            'FILENAME,COUNTRY_CODE)  values(:1, '
                            ':2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20,'
                            ' :21, :22, '
                            ':23, :24, :25, :26, :27, :28)')
                        cursor.executemany(None, rowBMD75)
                        cursor.prepare(
                            'insert into DATABASE_NAME.V_BSP_BKF81_FARE_CALCULATION(STANDARD_MESSAGE_IDENTIFIER,'
                            'SEQUENCE_NUMBER,STANDARD_NUMERIC_QUALIFIER,DATE_OF_ISSUE,TRANSACTION_NUMBER,'
                            'TICKET_DOCUMENT_NUMBER,CHECK_DIGIT,FARE_CALCULATION_SEQ_NUMBER,FARE_CALCULATION_AREA,'
                            'RESERVED_SPACE,FILENAME,COUNTRY_CODE)  '
                            'values(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12)')
                        cursor.executemany(None, rowBKF81)
                        cursor.prepare(
                            'insert into DATABASE_NAME.V_BSP_BCC82_ADDITIONAL_CARD_INFORMATION_RECORD('
                            'STANDARD_MESSAGE_IDENTIFIER, '
                            'SEQUENCE_NUMBER,STANDARD_NUMERIC_QUALIFIER,DATE_OF_ISSUE,TRANSACTION_NUMBER,'
                            'FORM_OF_PAYMENT_TYPE,FORM_OF_PAYMENT_TRANSACTION_ID,RESERVED_SPACE,FILENAME,COUNTRY_CODE ) '
                            'values(:1, :2, :3, :4, :5, '
                            ':6, :7, :8, :9, :10)')
                        cursor.executemany(None, rowBCC82)
                        cursor.prepare(
                            'insert into DATABASE_NAME.V_BSP_BCC83_CARD_AUTHENTICATION_INFORMATION_RECORD('
                            'STANDARD_MESSAGE_IDENTIFIER,SEQUENCE_NUMBER,STANDARD_NUMERIC_QUALIFIER'
                            'DATE_OF_ISSUE,TRANSACTION_NUMBER,FORM_OF_PAYMENT_TYPE,CARD_AUTHENTICATION_SEQUENCE'
                            'CARD_AUTHENTICATION_INFO,FILENAME,COUNTRY_CODE) values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)')
                        cursor.executemany(None, rowBCC83)

                        cursor.prepare(
                            'insert into DATABASE_NAME.V_BSP_BCX83_3DS_CARD_AUTHENTICATION_INFORMATION_RECORD('
                            'STANDARD_MESSAGE_IDENTIFIER, '
                            'SEQUENCE_NUMBER,STANDARD_NUMERIC_QUALIFIER,DATE_OF_ISSUE,TRANSACTION_NUMBER,'
                            'FORM_OF_PAYMENT_TYPE,CARD_AUTHENTICATION_SEQUENCE,SECURE_CARD_AUTHENTICATION,'
                            'FILENAME,COUNTRY_CODE )'
                            'values(:1, :2, '
                            ':3, :4, :5, '
                            ':6, :7, :8, :9, :10)')
                        cursor.executemany(None, rowBCX83)
                        cursor.prepare(
                            'insert into DATABASE_NAME.V_BSP_BKP84_FORM_OF_PAYMENT(STANDARD_MESSAGE_IDENTIFIER,'
                            'SEQUENCE_NUMBER,STANDARD_NUMERIC_QUALIFIER,DATE_OF_ISSUE,TRANSACTION_NUMBER,'
                            'FORM_OF_PAYMENT_TYPE,FORM_OF_PAYMENT_AMOUNT,FORM_OF_PAYMENT_ACCOUNT_NUMBER,EXPIRY_DATE,'
                            'EXTENDED_PAYMENT_CODE,APPROVAL_CODE,INVOICE_NUMBER,INVOICE_DATE,REMITTANCE_AMOUNT,'
                            'CARD_VERIFICATION_VALUE,RESERVED_SPACE,CURRENCY_TYPE,FILENAME,COUNTRY_CODE)  '
                            'values(:1, :2, :3, :4, :5, :6, :7, :8, '
                            ':9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19)')
                        cursor.executemany(None, rowBKP84)
                        cursor.prepare(
                            'insert into DATABASE_NAME.V_BSP_BOT93_OFFICE_SUBTOTALS_PER_TRANSACTION_CODE('
                            'STANDARD_MESSAGE_IDENTIFIER, '
                            'SEQUENCE_NUMBER,STANDARD_NUMERIC_QUALIFIER,AGENT_NUMERIC_CODE,REMITTANCE_PERIOD_ENDING_DATE, '
                            'GROSS_VALUE_AMOUNT,TOTAL_REMITTANCE_AMOUNT,TOTAL_COMMISSION_VALUE_AMOUNT,'
                            'TOTAL_TAX_MISCELLANEOUS_FEE,TRANSACTION_CODE,TOTAL_TAX_ON_COMMISSION,RESERVED_SPACE,'
                            'CURRENCY_TYPE,TICKET_DOCUMENT_NUMBER,FILENAME,COUNTRY_CODE) '
                            'values(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16)')
                        cursor.executemany(None, rowBOT93)

                        cursor.prepare(
                            'insert into DATABASE_NAME.V_BSP_BOT94_OFFICE_TOTALS_PER_CURRENCY_TYPE_RECORD('
                            'STANDARD_MESSAGE_IDENTIFIER, '
                            'SEQUENCE_NUMBER,STANDARD_NUMERIC_QUALIFIER,AGENT_NUMERIC_CODE,REMITTANCE_PERIOD_ENDING_DATE, '
                            'GROSS_VALUE_AMOUNT,TOTAL_REMITTANCE_AMOUNT,TOTAL_COMMISSION_VALUE_AMOUNT,'
                            'TOTAL_TAX_MISCELLANEOUS_FEE,TOTAL_TAX_ON_COMMISSION,RESERVED_SPACE,'
                            'CURRENCY_TYPE,TICKET_DOCUMENT_NUMBER,FILENAME,COUNTRY_CODE) '
                            'values(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15)')
                        cursor.executemany(None, rowBOT94)

                        cursor.prepare(
                            'insert into DATABASE_NAME.V_BSP_BCT95_BILLING_ANALYSIS_CYCLE_TOTAL('
                            'STANDARD_MESSAGE_IDENTIFIER, '
                            'SEQUENCE_NUMBER,STANDARD_NUMERIC_QUALIFIER,PROCESSING_DATE_ID, PROCESSING_CYCLE_ID,'
                            'OFFICE_COUNT,GROSS_VALUE_AMOUNT,TOTAL_REMITTANCE_AMOUNT,TOTAL_COMMISSION_VALUE_AMOUNT,'
                            'TOTAL_TAX_MISCELLANEOUS_FEE,TOTAL_TAX_ON_COMMISSION_AMOUNT,RESERVED_SPACE,'
                            'CURRENCY_TYPE,FILENAME,COUNTRY_CODE) '
                            'values(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15)')
                        cursor.executemany(None, rowBCT95)

                        cursor.prepare(
                            'insert into DATABASE_NAME.V_BSP_BFT99_FILE_TOTALS_PER_CURRENCY('
                            'STANDARD_MESSAGE_IDENTIFIER, '
                            'SEQUENCE_NUMBER,STANDARD_NUMERIC_QUALIFIER,BSP_ID,'
                            'OFFICE_COUNT,GROSS_VALUE_AMOUNT,TOTAL_REMITTANCE_AMOUNT,TOTAL_COMMISSION_VALUE_AMOUNT,'
                            'TOTAL_TAX_MISCELLANEOUS_FEE,TOTAL_TAX_ON_COMMISSION_AMOUNT,RESERVED_SPACE,'
                            'CURRENCY_TYPE,FILENAME,COUNTRY_CODE) '
                            'values(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14)')
                        cursor.executemany(None, rowBFT99)
                        cursor.prepare(
                            'insert into DATABASE_NAME.V_BSP_BKS24_TICKET_DOCUMENT_ID(STANDARD_MESSAGE_IDENTIFIER,'
                            'SEQUENCE_NUMBER,STANDARD_NUMERIC_QUALIFIER,DATE_OF_ISSUE,TRANSACTION_NUMBER,'
                            'TICKET_DOCUMENT_NUMBER,CHECK_DIGIT,COUPON_USE_INDICATOR,CONJUCTION_TICKET_INDICATOR,'
                            'AGENT_NUMERIC_CODE,REASON_FOR_ISSUANCE,TOUR_CODE,TRANSACTION_CODE,TRUE_ORIGIN,PNR_REFERENCE,'
                            'TIME_OF_ISSUE,CITY_CODE,RESERVED_SPACE,FILENAME,COUNTRY_CODE) values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,'
                            ':15,:16,:17,:18,:19,:20)')
                        cursor.executemany(None, rowBKS24)
                        cursor.prepare(
                            'insert into DATABASE_NAME.V_BSP_BKS30_STD_DOCUMENT_AMNTS(STANDARD_MESSAGE_IDENTIFIER,'
                            'SEQUENCE_NUMBER,STANDARD_NUMERIC_QUALIFIER,DATE_OF_ISSUE,TRANSACTION_NUMBER,'
                            'TICKET_DOCUMENT_NUMBER,CHECK_DIGIT,COMMISSIONABLE_AMOUNT,NET_FARE_AMOUNT,'
                            'TAX_MISCELLANEOUS_FEE_TYPE1,TAX_MISCELLANEOUS_FEE_AMOUNT1,TAX_MISCELLANEOUS_FEE_TYPE2,'
                            'TAX_MISCELLANEOUS_FEE_AMOUNT2,TAX_MISCELLANEOUS_FEE_TYPE3,TAX_MISCELLANEOUS_FEE_AMOUNT3,'
                            'TICKET_DOCUMENT_AMOUNT,RESERVED_SPACE,CURRENCY_TYPE,FILENAME,COUNTRY_CODE) values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,'
                            ':12,:13,:14,:15,:16,:17,:18,:19,:20)')
                        cursor.executemany(None, rowBKS30)
                        cursor.prepare(
                            'insert into DATABASE_NAME.V_BSP_BKS42_TAX_ON_COMMISSION(STANDARD_MESSAGE_IDENTIFIER,'
                            'SEQUENCE_NUMBER,STANDARD_NUMERIC_QUALIFIER,DATE_OF_ISSUE,TRANSACTION_NUMBER,'
                            'TICKET_DOCUMENT_NUMBER,CHECK_DIGIT,TAX_ON_COMMISSION_TYPE1,TAX_ON_COMMISSION_AMOUNT1,'
                            'TAX_ON_COMMISSION_TYPE2,TAX_ON_COMMISSION_AMOUNT2,TAX_ON_COMMISSION_TYPE3,'
                            'TAX_ON_COMMISSION_AMOUNT3,TAX_ON_COMMISSION_TYPE4,TAX_ON_COMMISSION_AMOUNT4,'
                            'RESERVED_SPACE,CURRENCY_TYPE,FILENAME,COUNTRY_CODE) values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,'
                            ':15,:16,:17,:18,:19)')
                        cursor.executemany(None, rowBKS42)
                        cursor.prepare(
                            'insert into DATABASE_NAME.V_BSP_BKS45_RELATED_TICKET_DOC(STANDARD_MESSAGE_IDENTIFIER,'
                            'SEQUENCE_NUMBER,STANDARD_NUMERIC_QUALIFIER,REMITTANCE_PERIOD_ENDING_DATE,TRANSACTION_NUMBER,'
                            'RELATED_TICKET_DOCUMENT_NUMBER,CHECK_DIGIT,WAIVER_CODE,REASON_FOR_MEMO_ISSUANCE,'
                            'RELATED_TICKET_DOCUMENT_COUPON,DATE_OF_ISSUE,RESERVED_SPACE,FILENAME,COUNTRY_CODE) values(:1,:2,:3,:4,:5,:6,:7,:8,:9,'
                            ':10,:11,:12,:13,:14) ')
                        cursor.executemany(None, rowBKS45)
                        cursor.prepare(
                            'insert into DATABASE_NAME.V_BSP_BAR65_ADDITIONAL_PAX_INF(STANDARD_MESSAGE_IDENTIFIER,'
                            'SEQUENCE_NUMBER,STANDARD_NUMERIC_QUALIFIER,DATE_OF_ISSUE,TRANSACTION_NUMBER,'
                            'TICKET_DOCUMENT_NUMBER,CHECK_DIGIT,PASSENGER_NAME,PASSENGER_SPECIFIC_DATA,DATE_OF_BIRTH,'
                            'PASSENGER_TYPE_CODE,RESERVED_SPACE,FILENAME,COUNTRY_CODE) values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14) ')
                        cursor.executemany(None, rowBAR65)
                        cursor.prepare(
                            'insert into DATABASE_NAME.V_BSP_BAR66_FORM_PAYMENT(STANDARD_MESSAGE_IDENTIFIER,SEQUENCE_NUMBER,'
                            'STANDARD_NUMERIC_QUALIFIER,DATE_OF_ISSUE,TRANSACTION_NUMBER,TICKET_DOCUMENT_NUMBER,CHECK_DIGIT,'
                            'FORM_OF_PAYMENT_SEQ_NUMBER,FORM_OF_PAYMENT_INFORMATION,RESERVED_SPACE,FILENAME,COUNTRY_CODE) values(:1,:2,:3,:4,:5,:6,'
                            ':7,:8,:9,:10,:11,:12)')
                        cursor.executemany(None, rowBAR66)
                        cursor.prepare(
                            'insert into DATABASE_NAME.V_BSP_BMD76_EMD_REMARKS(STANDARD_MESSAGE_IDENTIFIER,SEQUENCE_NUMBER,'
                            'STANDARD_NUMERIC_QUALIFIER,DATE_OF_ISSUE,TRANSACTION_NUMBER,TICKET_DOCUMENT_NUMBER,CHECK_DIGIT,'
                            'COUPON_NUMBER,EMD_REMARKS,RESERVED_SPACE,FILENAME,COUNTRY_CODE) values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12) ')
                        cursor.executemany(None, rowBMD76)
                        cursor.prepare('insert into DATABASE_NAME.V_BSP_BKS31_RECORD_LAYOUT(STANDARD_MESSAGE_IDENTIFIER,'
                                       'SEQUENCE_NUMBER,STANDARD_NUMERIC_QUALIFIER,DATE_OF_ISSUE,TRANSACTION_NUMBER,'
                                       'TICKET_DOCUMENT_NUMBER,CHECK_DIGIT,SEGMENT_IDENTIFIER1,COUPON_TAX_AIRPORT_CODE1,'
                                       'SEGMENT_TAX_AIRPORT_CODE1,COUPON_TAX_CODE1,COUPON_TAX_TYPE1,'
                                       'COUPON_TAX_REPORTED_AMOUNT1,COUPON_TAX_CURRENCY_TYPE1,COUPON_TAX_APPLICABLE_AMOUNT1,'
                                       'SEGMENT_IDENTIFIER2,COUPON_TAX_AIRPORT_CODE2,SEGMENT_TAX_AIRPORT_CODE2,'
                                       'COUPON_TAX_CODE2,COUPON_TAX_TYPE2,COUPON_TAX_REPORTED_AMOUNT2,'
                                       'COUPON_TAX_CURRENCY_TYPE2,COUPON_TAX_APPLICABLE_AMOUNT2,RESERVED_SPACE,CURRENCY_TYPE,FILENAME,COUNTRY_CODE) '
                                       'values(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, '
                                       ':18, :19, :20, :21, :22, :23, :24, :25, :26, :27)')
                        cursor.executemany(None, rowBKS31)
                        cursor.prepare('insert into DATABASE_NAME.V_BSP_BKS46_RECORD_LAYOUT(STANDARD_MESSAGE_IDENTIFIER,'
                                       'SEQUENCE_NUMBER,STANDARD_NUMERIC_QUALIFIER,DATE_OF_ISSUE,TRANSACTION_NUMBER,'
                                       'TICKET_DOCUMENT_NUMBER,CHECK_DIGIT,ORIGINAL_ISSUE_TICKET,ORIGINAL_ISSUE_LOCATION,'
                                       'ORIGINAL_ISSUE_DATE,ORIGINAL_ISSUE_AGENT,ENDORSMENTS,RESERVED_SPACE,FILENAME,COUNTRY_CODE) values(:1, :2, '
                                       ':3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15) ')
                        cursor.executemany(None, rowBKS46)
                        cursor.prepare('insert into DATABASE_NAME.V_BSP_BKS47_RECORD_LAYOUT(STANDARD_MESSAGE_IDENTIFIER,'
                                       'SEQUENCE_NUMBER,STANDARD_NUMERIC_QUALIFIER,DATE_OF_ISSUE,TRANSACTION_NUMBER,'
                                       'TICKET_NUMBER,CHECK_DIGIT,NETTING_TYPE1,NETTING_CODE1,NETTING_AMOUNT1,NETTING_TYPE2,'
                                       'NETTING_CODE2,NETTING_AMOUNT2,NETTING_TYPE3,NETTING_CODE3,NETTING_AMOUNT3,'
                                       'NETTING_TYPE4,NETTING_CODE4,NETTING_AMOUNT4,RESERVED_SPACE,CURRENCY_TYPE,FILENAME,COUNTRY_CODE) values(:1, '
                                       ':2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, '
                                       ':20, :21, :22, :23)')
                        cursor.executemany(None, rowBKS47)
                        cursor.prepare('insert into DATABASE_NAME.V_BSP_BKI61_RECORD_LAYOUT(STANDARD_MESSAGE_IDENTIFIER,'
                                       'SEQUENCE_NUMBER,STANDARD_NUMERIC_QUALIFIER,DATE_OF_ISSUE,TRANSACTION_NUMBER,'
                                       'TICKET_DOCUMENT_NUMBER,CHECK_DIGIT,SEGMENT_IDENTIFIER,UNTICKETED_POINT_AIRPORT,'
                                       'UP_DATE_OF_ARRIVAL,UP_LOCAL_TIME_ARRIVAL,UP_DATE_OF_DEPARTURE,UP_LOCAL_TIME_DEPATURE,'
                                       'UP_DEPATURE_EQUIPMENT_CODE,RESERVED_SPACE,FILENAME,COUNTRY_CODE) values(:1, :2, :3, :4, :5, :6, :7, :8, :9, '
                                       ':10, :11, :12, :13, :14, :15, :16, :17)')
                        cursor.executemany(None, rowBKI61)
                        cursor.prepare('insert into DATABASE_NAME.V_BSP_BKI62_RECORD_LAYOUT(STANDARD_MESSAGE_IDENTIFIER,'
                                       'SEQUENCE_NUMBER,STANDARD_NUMERIC_QUALIFIER,DATE_OF_ISSUE,TRANSACTION_NUMBER,'
                                       'DOCUMENT_NUMBER,CHECK_DIGIT,SEGMENT_IDENTIFIER,ORIGIN_AIRPORT,FLIGHT_DEPATURE_DATE,'
                                       'FLIGHT_DEPATURE_TIME,FLIGHT_DEPATURE_TERMINAL,DESTINATION_AIRPORT,'
                                       'FLIGHT_ARRIVAL_DATE,FLIGHT_ARRIVAL_TIME,FLIGHT_ARRIVAL_TERMINAL,RESERVED_SPACE,FILENAME,COUNTRY_CODE) '
                                       'values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19)')
                        cursor.executemany(None, rowBKI62)
                        cursor.prepare('insert into DATABASE_NAME.V_BSP_BAR67_RECORD_LAYOUT(STANDARD_MESSAGE_IDENTIFIER,'
                                       'SEQUENCE_NUMBER,STANDARD_NUMERIC_QUALIFIER,DATE_OF_ISSUE,TRANSACTION_NUMBER,'
                                       'TICKET_DOCUMENT_NUMBER,CHECK_DIGIT,TAX_INFO,TAX_INFO_IDENTIFIER,ADDITIONAL_TAX_INFO,'
                                       'RESERVED_SPACE,FILENAME,COUNTRY_CODE) values(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13)')
                        cursor.executemany(None, rowBAR67)




                    except cx_Oracle.IntegrityError:
                        f = open(logFileName, "a+")
                        f.write("THIS FILE DATA ALREADY INSERTED IN DATABASE: " + mfilename + "\n")
                        f.write("\n")

                    except cx_Oracle.DatabaseError:
                        f = open(logFileName, "a+")
                        f.write("INVALID NUMBER, STRING READ INSTEAD OF A NUMBER IN FILE : " + mfilename + "\n")
                        f.write("\n")

                    dbCon.commit()
            _mcount = _mcount + 1

        days = days + 1
    cursor.callproc(maltea_data_load)
    cursor.close()
    # cursor_select.close()
    dbCon.close()


def add_check(_msn, _msymb, _madg):
    if _msymb == '{':
        _madg = '0'
    if _msymb == 'A':
        _madg = '1'
    if _msymb == 'B':
        _madg = '2'
    if _msymb == 'C':
        _madg = '3'
    if _msymb == 'D':
        _madg = '4'
    if _msymb == 'E':
        _madg = '5'
    if _msymb == 'F':
        _madg = '6'
    if _msymb == 'G':
        _madg = '7'
    if _msymb == 'H':
        _madg = '8'
    if _msymb == 'I':
        _madg = '9'
    if _msymb == '}':
        _madg = '0'
        _msn = -1
    if _msymb == 'J':
        _madg = '1'
        _msn = -1
    if _msymb == 'K':
        _madg = '2'
        _msn = -1
    if _msymb == 'L':
        _madg = '3'
        _msn = -1
    if _msymb == 'M':
        _madg = '4'
        _msn = -1
    if _msymb == 'N':
        _madg = '5'
        _msn = -1
    if _msymb == 'O':
        _madg = '6'
        _msn = -1
    if _msymb == 'P':
        _madg = '7'
        _msn = -1
    if _msymb == 'Q':
        _madg = '8'
        _msn = -1
    if _msymb == 'R':
        _madg = '9'
        _msn = -1
    return _msn, _msymb, _madg


def error_handling():
    return 'Error: {}. {}, line: {}'.format(sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2].tb_lineno)


# def remove_none_elements_from_list(list):
#     return [e for e in list if (pd.notnull(e))]

def remove_none_elements_from_list(list):
    return [e for e in list if e is not None]
# dCon.close()b#
if __name__ == "__main__": main()
