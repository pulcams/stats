#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Processing productivity stats
Run with `python stats.py -m yyyymm` (e.g. python stats.py -m 201504)
Once all's done, the _out files (in ./out) are used to generate reports. This is done in MS Access for now (stats.accdb on lib-staff069). Authorities processing changed by naco committee 201803.
from 201412
pmg
"""
from collections import deque
from gsheets import Sheets

import argparse
import ConfigParser
import csv
import cx_Oracle
import datetime
import httplib2
import logging
import os
import pandas
import pickle
import re
import shutil
import sys, subprocess
import time
import datetime
from apiclient.http import MediaFileUpload
from collections import Counter
from googleapiclient.discovery import build
from oauth2client import file, client, tools
from oauth2client.service_account import ServiceAccountCredentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
# TODO ============================
# send tables to tsserver as csv
# document mounting shares with noserverino,nounix
# generate reports (jinja?)
# email / post (where?)
# sync changes to operators table (master copy on lib-tsserver)
# how-tos on tsserver -- legacy and current
# occaisional double entries into field, e.g. 'm l' for 902 sub_b
# =================================
config = ConfigParser.RawConfigParser()
config.read('vger.cfg')
user = config.get('database', 'user')
pw = config.get('database', 'pw')
sid = config.get('database', 'sid')
ip = config.get('database', 'ip')
nafprod_sheet = config.get('sheets','nafprod')
saco_sheet = config.get('sheets','saco')

http = httplib2.Http()
dsn_tns = cx_Oracle.makedsn(ip,1521,sid)
db = cx_Oracle.connect(user,pw,dsn_tns)
#today = time.strftime('%Y%m')
msg=''
f300=''
indir = "./in/"
archivedir = "./archive/"
outdir = "./out/"
all903mdb = '/Volumes/catalog/fpdb/new_page_1.mdb' # up-to-date data from Cataloguing Modification Reporting Form (903)
#allauthmdb = '/mnt/lib-tsserver/cams/for Lena/90x stats.mdb' # latest authorities data, manually entered by Lena
w7 = '/Volumes/Documents/petergreen/stats_temp/'

# logging
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',level=logging.INFO)
#changelog = logging.basicConfig(format='%(message)s',filename='logs/changes_'+today+'.log',level=logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

# argparse
parser = argparse.ArgumentParser(description='Process monthly stats files.')
parser.add_argument('-m','--month',type=str,dest="month",help="The month and year of the report needed, in the form YYYYMM'",required=True)
args = vars(parser.parse_args())

thisrun = args['month']
lastrun = '%s%02d01' % (thisrun[0:4],int(thisrun[4:6]) - 1)
lastauth = 'authorities_' + datetime.datetime.strptime(thisrun, '%Y%m').strftime('%Y-%m %b').replace(" ","_")

nafcsv = './in/NAFProduction%s.csv' % thisrun
sacocsv = './in/saco%s.csv' % thisrun

auths_out = outdir + 'auths_out.csv'

# run logger
run_logger = logging.getLogger('simple_logger')
hdlr_1 = logging.FileHandler('logs/run_'+thisrun+'.log')
hdlr_1.setFormatter(formatter)
run_logger.addHandler(hdlr_1)

# change logger
change_logger = logging.getLogger('simple_logger_2')
hdlr_2 = logging.FileHandler('logs/changes_'+thisrun+'.log')    
hdlr_2.setFormatter(formatter)
change_logger.addHandler(hdlr_2)

if not re.match('^\d{6}$',thisrun):
	print('Please enter the date in form YYYYDD')
	sys.exit() 

# get start and end dates for SQL query from -m argument
startdate = '%s/%02d/01' % (thisrun[0:4],int(thisrun[4:6]))
if (int(thisrun[4:6]) == 12): # if last month of year...
	enddate = '%s/%02d/01' % (int(thisrun[0:4])+1, 1)
else:	
	enddate = '%s/%02d/01' % (thisrun[0:4],int(thisrun[4:6]) + 1)

# operators
operators = []
with open('./lookups/operators.csv','rb') as ops:
	oreader = csv.reader(ops)
	for l in oreader:
		operators.append(l[3])
operators.append('vendor') # 'vendor' began summer 2016, checked w cjf

# legit pcc'ers
legit_pcc = []
with open('./lookups/legit_PCCers.csv','rb') as legit:
	lreader = csv.reader(legit)
	for l in lreader:
		legit_pcc.append(l[1])


def main():
	"""
	Call all of the functions sequentially
	"""
	#run_logger.info("start " + "=" * 25)
	#get_902()
	#get_904()
	#get_tables(all903mdb) # allauthmdb
	### get_naco()
	#get_nafprod()
	#get_saco()
	#clean_902()
	#clean_904()
	### process_authorities()
	process_authorities_gsheet()
	process_903()
	results2gsheets()
	cp_files()
	archive()
	run_logger.info("end " + "=" * 27)

#=======================================================================
# 902 report
#=======================================================================
def get_902():
	c = db.cursor()
	#SQL = """SELECT DISTINCT BIB_MASTER.BIB_ID as V_ID,
			#princetondb.GETBIBSUBFIELD(BIB_MASTER.BIB_ID, '902','a') as OP_ID,
			#princetondb.GETBIBSUBFIELD(BIB_MASTER.BIB_ID, '902','b') as Sub_B,
			#princetondb.GETBIBSUBFIELD(BIB_MASTER.BIB_ID, '902','d') as Sub_D,
			#princetondb.GETBIBSUBFIELD(BIB_MASTER.BIB_ID, '902','e') as Sub_E,
			#princetondb.GETBIBSUBFIELD(BIB_MASTER.BIB_ID, '902','f') as Sub_F,
			#princetondb.GETBIBSUBFIELD(BIB_MASTER.BIB_ID, '902','g') as Sub_G,
			#princetondb.GETBIBSUBFIELD(BIB_MASTER.BIB_ID, '902','6') as Sub_6,
			#princetondb.GETBIBSUBFIELD(BIB_MASTER.BIB_ID, '902','7') as Sub_7,
			#princetondb.GETBIBSUBFIELD(BIB_MASTER.BIB_ID, '902','s') as Sub_S
			#FROM BIB_MASTER LEFT JOIN BIB_HISTORY ON BIB_MASTER.BIB_ID = BIB_HISTORY.BIB_ID
			#WHERE
			#(((BIB_MASTER.CREATE_DATE) Between to_date ('%s', 'yyyy/mm/dd') And to_date ('%s', 'yyyy/mm/dd')) 
			#OR ((BIB_HISTORY.ACTION_DATE) Between to_date ('%s', 'yyyy/mm/dd') And to_date ('%s', 'yyyy/mm/dd')
			#AND (BIB_HISTORY.ACTION_TYPE_ID)<>1)) and rownum <= 3""" % (startdate,enddate,startdate,enddate)
	SQL = """SELECT DISTINCT BIB_MASTER.BIB_ID as V_ID,
			princetondb.GETALLBIBTAG(BIB_MASTER.BIB_ID, '902',2) as f902
			FROM BIB_MASTER LEFT JOIN BIB_HISTORY ON BIB_MASTER.BIB_ID = BIB_HISTORY.BIB_ID
			WHERE
			(((BIB_MASTER.CREATE_DATE) Between to_date ('%s', 'yyyy/mm/dd') And to_date ('%s', 'yyyy/mm/dd')) 
			OR ((BIB_HISTORY.ACTION_DATE) Between to_date ('%s', 'yyyy/mm/dd') And to_date ('%s', 'yyyy/mm/dd')
			AND (BIB_HISTORY.ACTION_TYPE_ID)<>1))""" % (startdate,enddate,startdate,enddate)
	print(SQL)
	c.execute(SQL)
	with open(indir + 'cat.csv',"wb+") as report:
		writer = csv.writer(report)
		header = ('V_ID','OP_ID','SUB_B','SUB_D','SUB_E','SUB_F','SUB_G','SUB_6','SUB_7','SUB_S')
		writer.writerow(header)
		for row in c:
			el = ''
			newrow = ''
			s902a = ''
			s902b = ''
			s902d = ''
			s902e = ''
			s902f = ''
			s902g = ''
			s9026 = ''
			s9027 = ''
			s902s = ''
			bibid = row[0]
			f902 = row[1]
			if row[1]:
				f902_full = row[1]
				f902s = f902_full.split('//')
				for f902 in f902s: 
					f902 = f902.replace('902:  :','').replace(' ','')
					f902_split = f902.split('$')[1:]
					#print(f902_split)
					if len(f902_split) > 1:
						sf = dict((el[0],el[1:]) for el in f902_split)
						if 'a' in sf:
							s902a = sf['a']
						if 'b' in sf:
							s902b = sf['b']
						if '6' in sf:
							s9026 = sf['6']
						if '7' in sf:
							s9027 = sf['7']
						if 'd' in sf:
							s902d = sf['d']
						if 'f' in sf:
							s902f = sf['f']
						if 'g' in sf:
							s902g = sf['g']	
						if 's' in sf:
							s902s = sf['s']	
						if 'e' in sf:
							s902e = sf['e']	
				
			newrow = [bibid,s902a,s902b,s902d,s902e,s902f,s902g,s9026,s9027,s902s]
			writer.writerow(newrow)
	c.close()
	msg = "Got 902 data!"
	logging.info(msg)
	#print(msg)


#=======================================================================
# 904 report
#=======================================================================
def get_904():
	c = db.cursor()
	#SQL = """SELECT DISTINCT BIB_MASTER.BIB_ID as V_ID,
		#princetondb.GETBIBSUBFIELD(BIB_MASTER.BIB_ID, '904','a') as OP_ID,
		#princetondb.GETBIBSUBFIELD(BIB_MASTER.BIB_ID, '904','b') as Sub_B,
		#princetondb.GETBIBSUBFIELD(BIB_MASTER.BIB_ID, '904','c') as Sub_C,
		#princetondb.GETBIBSUBFIELD(BIB_MASTER.BIB_ID, '904','e') as Sub_E,
		#princetondb.GETBIBSUBFIELD(BIB_MASTER.BIB_ID, '904','h') as Sub_H
		#FROM BIB_MASTER LEFT JOIN BIB_HISTORY ON BIB_MASTER.BIB_ID = BIB_HISTORY.BIB_ID
		#WHERE 
		#(((BIB_MASTER.CREATE_DATE) Between to_date ('%s', 'yyyy/mm/dd') And to_date ('%s', 'yyyy/mm/dd'))
		#OR (((BIB_HISTORY.ACTION_DATE) Between to_date ('%s', 'yyyy/mm/dd') And to_date ('%s', 'yyyy/mm/dd')) 
		#AND ((BIB_HISTORY.ACTION_TYPE_ID)<>1))) AND ROWNUM <= 3""" % (startdate,enddate,startdate,enddate)
	SQL = """SELECT DISTINCT BIB_MASTER.BIB_ID as V_ID,
		princetondb.GETALLBIBTAG(BIB_MASTER.BIB_ID, '904',2) as f904
		FROM BIB_MASTER LEFT JOIN BIB_HISTORY ON BIB_MASTER.BIB_ID = BIB_HISTORY.BIB_ID
		WHERE
		(((BIB_MASTER.CREATE_DATE) Between to_date ('%s', 'yyyy/mm/dd') And to_date ('%s', 'yyyy/mm/dd'))
		OR (((BIB_HISTORY.ACTION_DATE) Between to_date ('%s', 'yyyy/mm/dd') And to_date ('%s', 'yyyy/mm/dd')) 
		AND ((BIB_HISTORY.ACTION_TYPE_ID)<>1)))""" % (startdate,enddate,startdate,enddate)	
	print(SQL)	
	c.execute(SQL)
	with open(indir + 'acq.csv',"wb+") as report:
		writer = csv.writer(report)
		header = ('V_ID','OP_ID','SUB_B','SUB_C','SUB_E','SUB_H')
		writer.writerow(header)
		for row in c:
			el = ''
			newrow = ''
			s904a = ''
			s904b = ''
			s904c = ''
			s904e = ''
			s904h = ''
			bibid = row[0]
			if row[1]:
				f904_full = row[1]
				f904s = f904_full.split('//')
				for f904 in f904s: 
					f904 = f904.replace('904:  :','').replace(' ','')
					f904_split = f904.split('$')[1:]
					if len(f904_split) > 1:
						sf = dict((el[0],el[1:]) for el in f904_split)
						#print(sf)
						if 'a' in sf:
							s904a = sf['a']
						if 'b' in sf:
							s904b = sf['b']
						if 'h' in sf:
							s904h = sf['h']
						if 'c' in sf:
							s904c = sf['c']
						if 'e' in sf:
							s904e = sf['e']
							
			newrow = [bibid,s904a,s904b,s904c,s904e,s904h]
			writer.writerow(newrow)
	c.close()
	msg = "Got 904 data!"
	logging.info(msg)


#=======================================================================
# clean the 902 report
#=======================================================================
def clean_902():
	"""
	Clean 902 report
	"""
	with open(indir + 'cat.csv',"rb") as infile, open(outdir + '902_out.csv','wb+') as outfile:
		reader = csv.reader(infile)
		writer = csv.writer(outfile)
		next(reader, None)  # skip the headers
		header = ('V_ID', 'OP_ID', 'SUB_B', 'SUB_6', 'SUB_7', 'SUB_D', 'SUB_E', 'SUB_F', 'SUB_G', 'SUB_S')
		writer.writerow(header)
		#====================
		# 902 report fields
		#====================
		for line in reader:
			bbid = line[0]
			opid = line[1]
			sub_b = line[2]
			sub_6 = line[7]
			sub_7 = line[8]
			sub_d = line[3]
			sub_e = line[4]
			sub_f = line[5]
			sub_g = line[6]
			sub_s = line[9]
			if sub_e.startswith(thisrun):
				#===================
				# 902$6 from ldr/06
				#===================
				if sub_6 == '' or sub_6 not in ('a','c','d','e','f','g','i','j','k','m','o','p','r','t'):
					c = db.cursor()
					SQL = """SELECT to_char(substr(princetondb.GETBIBBLOB(BIB_TEXT.BIB_ID),7,1)) FROM BIB_TEXT 
					WHERE BIB_ID = '%s'""" % bbid
					c.execute(SQL)
					for row in c:
						msg = '%s,902$6,%s,%s' % (bbid,sub_6,''.join(row))
						change_logger.info(msg)
						sub_6 = ''.join(row)
					c.close()
				#===================
				# 902$7 from ldr/07
				#===================
				if sub_7 == '' or sub_7 not in ('a','b','c','d','i','m','s'):
					c = db.cursor()
					SQL = """SELECT to_char(substr(princetondb.GETBIBBLOB(BIB_TEXT.BIB_ID),8,1)) FROM BIB_TEXT 
					WHERE BIB_ID = '%s'""" % bbid
					c.execute(SQL)
					for row in c:
						msg = '%s,902$7,%s,%s' % (bbid,sub_7,''.join(row))
						change_logger.info(msg)
						sub_7 = ''.join(row)
					c.close()
					
				#===================
				# 902$b type of cataloging (chosen manually by cataloger)
				#===================
				if (sub_b != '' and sub_b not in ('b','c','l','m','o','r','s','x','z')) or (sub_b == ''):
					# TODO: refine this: checking 040 and 035
					c = db.cursor()
					toc = 'm' # type of cataloging (m as default)
					SQL = """SELECT princetondb.GETALLBIBTAG(BIB_TEXT.BIB_ID, '040',2) as f040, princetondb.GETALLBIBTAG(BIB_TEXT.BIB_ID, '035',2) as f035 FROM BIB_TEXT 
					WHERE BIB_ID = '%s'""" % bbid
					c.execute(SQL)
					try:
						for row in c:
							f040 = ''.join(row)
							#print(f040)
							if '$aDLC' in f040 and '$cPUL' in f040: 
								toc = 'l'
							elif f040.count('$d') > 1 or '(OCoLC)' in f040:
								toc = 'm'
							elif '$aNjP' in f040:
								toc = 'o'
							msg = '%s,902$b,%s,%s' % (bbid,sub_b,toc)
							change_logger.info(msg)
					except:
						toc = 'm' # guessing that it's member if no 040 TODO - refine this
							
						sub_b = toc
					c.close()

				#===================
				# 902$e
				#===================
				# regex to cut off all but first 8 chars (if numeric)
				if sub_e != '' and not re.match('^\d{8}$',sub_e):
					msg = '%s,902$e,%s,%s' % (bbid,sub_e,sub_e[0:8])
					change_logger.info(msg)
					sub_e = sub_e[0:8]
				
				#===================
				# 902$a operator id
				#===================
				# check against operators table => vger history
				if (opid != '' and opid not in (operators)) or (opid == ''): # this covers numbers and other random entries, and blank opids
					this902e = sub_e
					c = db.cursor()
					SQL = """SELECT DISTINCT BIB_HISTORY.OPERATOR_ID
					FROM BIB_MASTER LEFT JOIN BIB_HISTORY ON BIB_MASTER.BIB_ID = BIB_HISTORY.BIB_ID
					WHERE
					(((to_char(BIB_MASTER.CREATE_DATE,'yyyymmdd')) = '%s') 
					OR ((to_char(BIB_HISTORY.ACTION_DATE,'yyyymmdd')) = '%s' 
					AND (BIB_HISTORY.ACTION_TYPE_ID)<>1))
					AND
					BIB_HISTORY.BIB_ID = '%s'""" % (this902e,this902e,bbid)
					#print(SQL)
					c.execute(SQL)
					for row in c:
						msg = '%s,902$a,%s,%s' % (bbid,opid,''.join(row))
						change_logger.info(msg)
						opid = ''.join(row)
					c.close()
				
				#===================
				# 902$d 
				#===================
				# chosen by cataloger -- how to check? 007? These are rarely if ever blank. Most common value is "v"
				if sub_d == '' or sub_d not in ['a','c','d','e','f','g','l','m','r','s','t','v','w']:
					msg = '%s,902$d,%s,%s' % (bbid,sub_d,'v')
					change_logger.info(msg)
					sub_d = "v"
					
				#===================
				# 902$f entered manually
				#===================
				# numbers only, if no number == 1
				non_num = re.compile(r'[^\d]+')
				if sub_f == '':
					msg = '%s,902$f,%s,%s' % (bbid,sub_f,'1')
					sub_f = 1 
					change_logger.info(msg)
				elif not re.match(r'^[\d]+$', sub_f):
					msg = '%s,902$f,%s,%s' % (bbid,sub_f,non_num.sub('',sub_f))
					sub_f = non_num.sub('',sub_f)
					change_logger.info(msg)
					
				#===================
				# 902$g
				#===================
				# values are 'p' or '?'
				if sub_g not in ['p','?']:
					sub_g = '?'
					#print('%s,902$g,%s,%s' % (bbid,sub_g,'?'))
				
				#===================
				# 902$s
				#===================
				# blanks should be '?'
				if sub_s == '':
					sub_s = '?'
					#print('%s,902$s,%s,%s' % (bbid,sub_s,'?'))
				
				#=======================
				# Eliminate illegit PCC
				#=======================
				# read in file of legit pcc'ers	
				# not in legit pcc and subg=p, set subb=m and subg=?
				if sub_g == 'p' and opid not in (legit_pcc):
					msg = '%s, %s, pcc $g%s $b%s, $g%s $b%s' % (bbid,opid,sub_g,sub_b,'?','m')
					change_logger.info(msg)
					sub_g = '?'
					if sub_b == 'o':
						sub_b = 'm'
					
				newline = bbid, opid, sub_b, sub_6, sub_7, sub_d, sub_e, sub_f, sub_g, sub_s
				writer.writerow(newline)
	msg = '902 report is clean!'
	run_logger.info(msg)


#=======================================================================
# clean the 904 report
#=======================================================================
def clean_904():
	"""
	Clean up 904 report.
	"""
	f040 = ''
	with open(indir + 'acq.csv',"rb") as infile, open(outdir + '904_out.csv','wb+') as outfile:
		reader = csv.reader(infile)
		writer = csv.writer(outfile)
		next(reader, None)  # skip the headers
		header = ('V_ID','OP_ID','SUB_B','SUB_C','SUB_E','SUB_H')
		writer.writerow(header)
		#=====================
		# 904 report fields
		#=====================
		for line in reader:
			bbid = line[0]
			opid = line[1]
			sub_b = line[2]
			sub_c = line[3]
			sub_e = line[4]
			sub_h = line[5]
			opidv = ''
			
			#===================
			# 904$e date
			#===================
			# same as 902$e
			if sub_e != '' and not re.match('^\d{8}$',sub_e):
				if len(sub_e[0:8]) < 8: # sometimes dates are truncated
					bad_date = sub_e
					c = db.cursor()
					SQL = """SELECT DISTINCT to_char(BIB_HISTORY.ACTION_DATE,'yyyymmdd')
					FROM BIB_MASTER LEFT JOIN BIB_HISTORY ON BIB_MASTER.BIB_ID = BIB_HISTORY.BIB_ID
					WHERE
					BIB_HISTORY.OPERATOR_ID = '%s'
					AND BIB_HISTORY.BIB_ID = '%s'""" % (opid,bbid)
					c.execute(SQL)
					for row in c:
						sub_e = ''.join(row)
					c.close()
					emsg = '%s,904$e,%s,%s' % (bbid, bad_date, sub_e)
					#change_logger.info(msg)
					#print(msg)
				else:
					num_only = re.compile(r'[^\d.]+')
					sub_e = num_only.sub('',sub_e)
					emsg = '%s,904$e,%s,%s' % (bbid,sub_e,sub_e[0:8])
					sub_e = sub_e[0:8]
				change_logger.info(emsg)
				#print(emsg)
			
			if sub_e.startswith(thisrun):
				#===================
				# 904$a initials
				#===================
				# check against operators table => vger history
				if ((opid != '' and opid.strip() not in (operators)) or (opid == '')):
					c = db.cursor()
					SQL = """SELECT DISTINCT BIB_HISTORY.OPERATOR_ID
					FROM BIB_MASTER LEFT JOIN BIB_HISTORY ON BIB_MASTER.BIB_ID = BIB_HISTORY.BIB_ID
					WHERE
					(((BIB_MASTER.CREATE_DATE) Between to_date ('%s', 'yyyy/mm/dd') And to_date ('%s', 'yyyy/mm/dd')) 
					OR ((BIB_HISTORY.ACTION_DATE) Between to_date ('%s', 'yyyy/mm/dd') And to_date ('%s', 'yyyy/mm/dd')
					AND (BIB_HISTORY.ACTION_TYPE_ID)<>1))
					AND
					BIB_HISTORY.BIB_ID = '%s'""" % (startdate,enddate,startdate,enddate,bbid)
					c.execute(SQL)
					for row in c:
						opidv = ''.join(row)
					c.close()
					amsg = '%s,904$a,%s,%s' % (bbid,opid,opidv)
					change_logger.info(amsg)
					#print(msg)
				
				#===================
				# 904$b type of receipt
				#===================
				# operators table unit (order* or not)
				bmsg = bbid + ' $b is missing'
				if sub_b.strip() not in ['a','d','g','m','o']:
					with open('./lookups/operators.csv','rb') as ops:
						oreader = csv.reader(ops)
						for l in oreader:
							if opidv == l[3] or opid == l[3]:
								unit = l[4] # get unit of operator
								if unit.startswith("order"):
									bmsg = '%s,904$b,%s,%s' % (bbid, sub_b, 'o')
									sub_b = 'o'
								else:
									bmsg = '%s,904$b,%s,%s' % (bbid, sub_b, 'a')
									sub_b = 'a'
					change_logger.info(bmsg)
				
				#===================
				# 904$c format of record
				#===================
				# 300 'pages' 'p.' #TODO refine this
				f300 = ''
				if sub_c == '' or sub_c.strip() not in ['a','b','c','d','f','j','m','p','r','s','t','v','x']:
					c = db.cursor()
					SQL = """SELECT princetondb.getAllBibTag(BIB_TEXT.BIB_ID,'300',2) as f300
					FROM BIB_TEXT
					WHERE
					BIB_TEXT.BIB_ID = '%s'""" % bbid
					c.execute(SQL)
					for row in c:
						if row[0] is not None:
							f300 = ''.join(row)
					c.close()
					if (f300 != '') and (' p.' in f300 or 'page' in f300):
						cmsg = '%s,904$c,%s,%s' % (bbid,sub_c,'b ' + f300)
						sub_c = 'b'
						change_logger.info(cmsg)
						#print(msg)
					else:
						cmsg = '%s,904$c,%s,%s' % (bbid, sub_c, 'b') # very cheap, but equivalent to quick visual review / guesswork
						sub_c = 'b'
						change_logger.info(cmsg)
						#print(msg)
					
						
				#===================
				# 904$h method of creation
				#===================
				# m or n - most are m; check length of 040
				# if NjP not in 040 = 'm'
				if sub_h == '' or sub_h.strip() not in ['m','n']:
					c = db.cursor()
					SQL = """SELECT princetondb.getAllBibTag(BIB_ID,'040',2) as f040
					FROM BIB_TEXT
					WHERE
					BIB_ID = '%s'""" % bbid
					c.execute(SQL)
					for row in c:
						if row[0] is not None:
							f040 = ''.join(row)
					c.close()
					if f040 == '' or '$aNjP' not in f040: # TODO: refine this
						hmsg = '%s,904$h,%s,%s' % (bbid,sub_h,'m')
						sub_h = 'm'
					else:
						hmsg = '%s,904$h,%s,%s' % (bbid,sub_h,'n')
						sub_h = 'n'
					change_logger.info(hmsg)
					#print(msg)
				
				newline = bbid, opid, sub_b, sub_c, sub_e, sub_h 
				writer.writerow(newline)
	msg = '904 report is clean!'
	run_logger.info(msg)
	#print(msg)


#=======================================================================
# process authorities report
#=======================================================================
# def process_authorities():
	# """
	# Filter last month's authorities report.
	# The Google Sheets version has 2 header rows as of 201801
	# """
	# with open(indir + lastauth + '.csv','rb') as auths, open(outdir + 'auths_out.csv','wb+') as authsout:
		# reader = csv.reader(auths)
		# writer = csv.writer(authsout)
		# next(reader, None)
		# header = ('initials','NACO new','NACO updates','SACO','NACO Series','NACO Series Updates') # removing 053 201808
		# writer.writerow(header)
		# for i,line in enumerate(reader):
			# if i>=2:
				# order = '0' # useless really, just keeping for convenience
				# opid = line[0].lower()
				# naco = line[-5:][0]
				# updates = line[-5:][1]
				# saco = line[-5:][2]
				# naco_series = line[-5:][3]
				# updates_series = line[-5:][4]
				# #f053 = line[-5:][5] # removed 201807
				
				# # add the NACO and update figures together (no longer needed 201803)
				# # naco = int(naco1) + int(naco2) + int(naco3) + int(naco4) + int(naco5)
				# # updates = int(update1) + int(update2) + int(update3) + int(update4) + int(update5)
				
				# line = order, opid, naco, updates, saco, naco_series, updates_series
				# print(line)
				# writer.writerow(line)
			
	# msg = 'authorities table is ready for manual additions'
	# run_logger.info(msg)
	# print(msg)

def process_authorities_gsheet():
	"""
	Filter NAFProduction Google Sheet
	
	'New' New NARs are counted from OTHER + ADD
	'Updates' Updated NARs are counted from OTHER + RPL
	'Series' New SARs are counted from SERIES + ADD
	'Updates series' Updated SARs are counted from SERIES + RPL
	'Saco' from Yang 'how'
	
	"""
	with open(nafcsv,'rb') as auths:
		reader = csv.reader(auths)
		vgerids = {}
		naco = 0
		naco_update = 0
		series = 0
		series_update = 0
		
		next(reader, None)
		for line in reader:
			opid = line[0].lower() 
			vgerids[opid] = {'naco':naco,'naco_update':naco_update,'series':series,'series_update':series_update} # inner dict

	with open(nafcsv,'rb') as auths:
		reader = csv.reader(auths)
		for line in reader:
			for i in vgerids:
				opid = line[0].lower()
				if opid == i:
					if line[2] == 'OTHER' and line[3] == 'ADD':
						vgerids[i]['naco'] += 1
					elif line[2] == 'OTHER' and line[3] == 'RPL':
						vgerids[i]['naco_update'] += 1
					elif line[2] == 'SERIES' and line[3] == 'ADD':
						vgerids[i]['series'] += 1
					elif line[2] == 'SERIES' and line[3] == 'RPL':
						vgerids[i]['series_update'] += 1
				
	table = (pandas.DataFrame(vgerids).T)
	table.to_csv(auths_out)

	# replace the first line (start with a comma)
	with open(auths_out,'r') as src:
		lines = src.readlines()
	header = 'vgerid,NACO new,NACO updates,NACO Series,NACO Series Updates\n'
	lines[0] = header
	with open(auths_out,'w') as f:
		f.writelines(lines)

	# merge the saco sheet with the nafprod sheet, 'df'=dataframe
	df1=pandas.read_csv(auths_out)
	df2=pandas.read_csv(sacocsv, index_col="vgerid")
	df2['SACO'] = df2[['LCSH New','LCSH Updates','LCC New','LCC Updates']].sum(axis=1)
	df2 = df2.drop(df2.columns[[0,1,2,3]],axis=1).astype(int)
	df2.to_csv(indir + 'saco_summed.csv',header=True,index="vgerid")
	df3=pandas.read_csv(indir + 'saco_summed.csv', index_col="vgerid")
	combo=pandas.merge(df1,df3,on="vgerid")
	combo.to_csv(auths_out)
	msg = 'authorities table is ready for manual additions'
	run_logger.info(msg)
	print(msg)


#=======================================================================
# process 903 report
#=======================================================================
def process_903():
	"""
	Filter 903 report. 
	"""
	try: 
		lastid = get_last_row('./archive/'+lastrun+'_903.csv')
		src = lastrun
	except:
		lastid = get_last_row(outdir + '903_out.csv')
		src = outdir + '903_out.csv'
	lastidchk = raw_input("Last id in "+src+" is " + lastid[0]+'. If this is not correct, enter the last id from the previous 903 report. Otherwise, just hit enter.')
	if lastidchk == '':
		lastid = lastid[0]
	else:
		lastid = lastidchk
	lastdate = raw_input("What's the end date (dpk and nb made large entries) yyyymmdd (inclusive)? ")
	if not re.match('^\d+$',lastid):
		sys.exit('Id needs to be an integer.')
	msg = 'last 903 id was ' + lastid
	run_logger.info(msg)
	
	with open(indir + 'Results.csv','rb') as f903, open(outdir + '903_out.csv','wb+') as f903out:
		reader = csv.reader(f903)
		writer = csv.writer(f903out)
		next(reader, None)
		header = ('ID', 'Initials', 'Sub_B','Sub_C', 'Num_Pieces', 'Note', 'Remote_computer_name', 'User_name', 'Browser_type', 'Timestamp')
		writer.writerow(header)
		for line in reader:
			#results2gsheets(line)
			thisid = line[0]
			initials = line[1].lower()
			timestamp = line[9]
			d = datetime.datetime.strptime(timestamp, '%m/%d/%y %H:%M:%S').strftime('%Y%m%d')
			if (int(d) <= int(lastdate)) and (int(thisid) > int(lastid)):
				#print('true', d, '<=',lastdate,'   ',thisid, lastid)
				writer.writerow(line)
	msg = '903 table has been filtered'
	run_logger.info(msg)


def archive():
	"""
	archive reports; move output tables from temp/ to archive/
	"""
	try:
		for report in ['902','903','904','auths']:
			temp = outdir + report + '_out.csv'
			store = archivedir + thisrun + '_' + report + '.csv'
			shutil.copy(temp, store)
	except:
		etype,evalue,etraceback = sys.exc_info()
		print(evalue)
	
	msg = 'reports archived'
	run_logger.info(msg)
	print(msg)


def get_last_row(csv_filename):
	"""
	Grabbed from here: http://stackoverflow.com/questions/20296955/reading-last-row-from-csv-file-python-error
	For getting last row of 903 report.
	"""
	with open(csv_filename, 'rb') as f:
		return deque(csv.reader(f), 1)[0]
	print ', '.join(get_last_row(filename))


def get_tables(*mdbs):
	"""
	Get latest 903 and authorities tables for processing
	"""
	for mdb in mdbs:
		print mdb
		last = ''
		lasttbl = ''
		# Get the list of table names with "mdb-tables"
		#table_names = subprocess.Popen(["mdb-tables", "-1", mdb], stdout=subprocess.PIPE).communicate()[0]	
		#tables = table_names.split('\n')
		
		#table = 'Results'
		#table_names = subprocess.check_output(["mdb-export", mdb, table])
		#tables = table_names.decode().split('\n')
	
		#if '90x' in mdb: # the database is called "90x stats"
		#	last = datetime.datetime.strptime(thisrun, '%Y%m').strftime('%Y-%m %b')
		#	lasttbl = 'authorities ' + last
	
		## Dump each table as a CSV file using "mdb-export",
		## converting " " in table names to "_" for the CSV filenames.     
		table = 'Results' # Results is 903s
		filename = table + ".csv"
		thisfile = open(indir + filename, 'w')
		contents = subprocess.Popen(["mdb-export", mdb, table],stdout=subprocess.PIPE).communicate()[0]
		thisfile.write(contents)
		thisfile.close()
		msg = 'got \'' + table + '\' from ' + mdb
		run_logger.info(msg)


def results2gsheets():
	"""
	Sent Results to Google Sheet which is datasource for a Tableau viz, for mwc
	"""
	gauth = GoogleAuth()
	gauth.LocalWebserverAuth()
	
	drive = GoogleDrive(gauth)
	
	filename = "Results"
	files = drive.ListFile({'q': "title='{}' and trashed=false".format(filename)}).GetList()
	if files:
	    file1 = files[0]
	else:
	    file1 = drive.CreateFile({'title': filename, 'mimeType': 'text/csv'})
	
	os.chdir('./in')
	file1.SetContentFile(filename+'.csv')
	file1.Upload({'convert':True})
	msg = 'Uploaded %s' % file1
	os.chdir('..')
	print(msg)
	logging.info(msg)


def get_naco():
	"""
    Get NACO stats from Google Sheets
	Requires the Google Drive api these instructions https://developers.google.com/drive/v3/web/quickstart/python
	Uses gsheets. As of 201902 have to share with personal gmail account.
	"""
	sheets = Sheets.from_files('./client_secret.json','./storage.json')

	#fileId = '1Ntmb0fJc5-0Ul1ShucPu4aOymR2zIcw456rsXish5Lk'
	fileId = '1mVaWtiVj088WPt0aed6v2D_iVwGbB3ux-tJ5t-c_GCU'

	url = 'https://docs.google.com/spreadsheets/d/' + fileId
	
	s = sheets.get(url)

	nacocsv = indir + lastauth + '.csv'

	sheet_index = int(thisrun[4:6]) - 1

	s.sheets[sheet_index].to_csv(nacocsv,encoding='utf-8',dialect='excel')

	msg = 'NACO stats saved to csv'
	print(msg)
	logging.info(msg)


def get_nafprod():
	"""
	This is the replacement for get_naco()
	"""
	sheets = Sheets.from_files('./client_secret.json','./storage.json')
	fileId = nafprod_sheet
	url = 'https://docs.google.com/spreadsheets/d/' + fileId
	s = sheets.get(url)
	sheet_index = int(thisrun[-2:]) # sheet index should equal month
	
	s.sheets[sheet_index].to_csv(nafcsv,encoding='utf-8',dialect='excel')

	msg = 'NAFProduction Google Sheet for %s saved to csv' % thisrun
	print(msg)
	logging.info(msg)


def get_saco():
	"""
	SACO is recorded in a separate Google Sheet as of 201905
	"""
	sheets = Sheets.from_files('./client_secret.json','./storage.json')
	fileId = saco_sheet
	url = 'https://docs.google.com/spreadsheets/d/' + fileId
	s = sheets.get(url)
	sheet_index = int(thisrun[-2:]) # sheet index should equal month
	
	s.sheets[sheet_index].to_csv(sacocsv,encoding='utf-8',dialect='excel')

	msg = 'SACO Google Sheet for %s saved to csv' % thisrun
	print(msg)
	logging.info(msg)


def cp_files():
	"""
	move the cleaned files (902, 903, 904, auth) to Windows 7 machine,
	where reports will be generated using MS Access (boo!)
	"""
	print(outdir)
	src = os.listdir(outdir)
	for f in src:
		print(outdir + f,w7)
		shutil.copy(outdir + f, w7)
		msg = 'moved ' + f
		run_logger.info(msg)


if __name__ == "__main__":
	main()
