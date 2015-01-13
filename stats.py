#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Processing productivity stats
At this stage (as of 2015/01), the following tables need to be in ./data/ as csv files *before* running this script.
(1) Results.csv from \\lib-terminal\catalog\fpdb as 'all903.csv'
(2) latest 90x report from \\lib-tsserver\CaMS\for Lena\90x stats.mdb as 'authoritiesyyyymm.csv'
Once these have been gathered, run with python stats.py -m yyyymm (e.g. python stats.py -m 201504)
Once all's done, the _out files (in ./temp) are used to generate reports. This is done in MS Access for now (stats.accdb on lib-staff069).
from 2014/12
pmg
"""
from collections import deque
import argparse
import ConfigParser
import csv
import cx_Oracle
import datetime
import logging
import re
import shutil
import sys
import time
import datetime

# TODO ============================
# send tables to tsserver (requires ms access interaction)
# generate reports (jinja?)
# email / *post
# sync changes to operators table (master copy on lib-tsserver)
# =================================
config = ConfigParser.RawConfigParser()
config.read('vger.cfg')
user = config.get('database', 'user')
pw = config.get('database', 'pw')
sid = config.get('database', 'sid')
ip = config.get('database', 'ip')

dsn_tns = cx_Oracle.makedsn(ip,1521,sid)
db = cx_Oracle.connect(user,pw,dsn_tns)
today = time.strftime('%Y%m')
msg=''
f300=''
datadir = "./data/"
archivedir = "./archive/"

# logging
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',level=logging.INFO)
#changelog = logging.basicConfig(format='%(message)s',filename='logs/changes_'+today+'.log',level=logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

# run logger
run_logger = logging.getLogger('simple_logger')
hdlr_1 = logging.FileHandler('logs/run_'+today+'.log')
hdlr_1.setFormatter(formatter)
run_logger.addHandler(hdlr_1)

# change logger
change_logger = logging.getLogger('simple_logger_2')
hdlr_2 = logging.FileHandler('logs/changes_'+today+'.log')    
hdlr_2.setFormatter(formatter)
change_logger.addHandler(hdlr_2)

# argparse
parser = argparse.ArgumentParser(description='Process monthly stats files.')
parser.add_argument('-m','--month',type=str,dest="month",help="The month and year of the report needed, in the form YYYYMM'",required=True)
args = vars(parser.parse_args())

thisrun = args['month']
lastrun = '%s%02d01' % (thisrun[0:4],int(thisrun[4:6]) - 1)
print(lastrun)

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
		operators.append(l[2])

# legit pcc'ers
legit_pcc = []
with open('./lookups/legit_PCCers.csv','rb') as legit:
	lreader = csv.reader(legit)
	for l in lreader:
		legit_pcc.append(l[1])

def main():
	run_logger.info("start " + "=" * 25)
	#get_902()
	#get_904()
	#clean_902()
	#clean_904()
	#process_authorities()
	process_903()
	#archive()
	run_logger.info("end " + "=" * 27)

#=======================================================================
# 902 report
#=======================================================================
def get_902():
	c = db.cursor()
	SQL = """SELECT DISTINCT BIB_MASTER.BIB_ID as V_ID,
			princetondb.GETBIBSUBFIELD(BIB_MASTER.BIB_ID, '902','a') as OP_ID,
			princetondb.GETBIBSUBFIELD(BIB_MASTER.BIB_ID, '902','b') as Sub_B,
			princetondb.GETBIBSUBFIELD(BIB_MASTER.BIB_ID, '902','d') as Sub_D,
			princetondb.GETBIBSUBFIELD(BIB_MASTER.BIB_ID, '902','e') as Sub_E,
			princetondb.GETBIBSUBFIELD(BIB_MASTER.BIB_ID, '902','f') as Sub_F,
			princetondb.GETBIBSUBFIELD(BIB_MASTER.BIB_ID, '902','g') as Sub_G,
			princetondb.GETBIBSUBFIELD(BIB_MASTER.BIB_ID, '902','6') as Sub_6,
			princetondb.GETBIBSUBFIELD(BIB_MASTER.BIB_ID, '902','7') as Sub_7,
			princetondb.GETBIBSUBFIELD(BIB_MASTER.BIB_ID, '902','s') as Sub_S
			FROM BIB_MASTER LEFT JOIN BIB_HISTORY ON BIB_MASTER.BIB_ID = BIB_HISTORY.BIB_ID
			WHERE
			(((BIB_MASTER.CREATE_DATE) Between to_date ('%s', 'yyyy/mm/dd') And to_date ('%s', 'yyyy/mm/dd')) 
			OR ((BIB_HISTORY.ACTION_DATE) Between to_date ('%s', 'yyyy/mm/dd') And to_date ('%s', 'yyyy/mm/dd')
			AND (BIB_HISTORY.ACTION_TYPE_ID)<>1))""" % (startdate,enddate,startdate,enddate)
	c.execute(SQL)
	with open(datadir + 'cat.csv',"wb+") as report:
		writer = csv.writer(report)
		header = ('V_ID','OP_ID','SUB_B','SUB_D','SUB_E','SUB_F','SUB_G','SUB_6','SUB_7','SUB_S')
		writer.writerow(header)
		for row in c:
			writer.writerow(row)
	c.close()
	msg = "Got 902 data!"
	logging.info(msg)
	print(msg)

#=======================================================================
# 904 report
#=======================================================================
def get_904():
	c = db.cursor()
	SQL = """SELECT DISTINCT BIB_MASTER.BIB_ID as V_ID,
		princetondb.GETBIBSUBFIELD(BIB_MASTER.BIB_ID, '904','a') as OP_ID,
		princetondb.GETBIBSUBFIELD(BIB_MASTER.BIB_ID, '904','b') as Sub_B,
		princetondb.GETBIBSUBFIELD(BIB_MASTER.BIB_ID, '904','c') as Sub_C,
		princetondb.GETBIBSUBFIELD(BIB_MASTER.BIB_ID, '904','e') as Sub_E,
		princetondb.GETBIBSUBFIELD(BIB_MASTER.BIB_ID, '904','h') as Sub_H
		FROM BIB_MASTER LEFT JOIN BIB_HISTORY ON BIB_MASTER.BIB_ID = BIB_HISTORY.BIB_ID
		WHERE 
		(((BIB_MASTER.CREATE_DATE) Between to_date ('%s', 'yyyy/mm/dd') And to_date ('%s', 'yyyy/mm/dd'))
		OR (((BIB_HISTORY.ACTION_DATE) Between to_date ('%s', 'yyyy/mm/dd') And to_date ('%s', 'yyyy/mm/dd')) 
		AND ((BIB_HISTORY.ACTION_TYPE_ID)<>1)))""" % (startdate,enddate,startdate,enddate)
	c.execute(SQL)
	with open(datadir + 'acq.csv',"wb+") as report:
		writer = csv.writer(report)
		header = ('V_ID','OP_ID','SUB_B','SUB_C','SUB_E','SUB_H')
		writer.writerow(header)
		for row in c:
			writer.writerow(row)
	c.close()
	msg = "Got 904 data!"
	logging.info(msg)
	print(msg)

#=======================================================================
# clean the 902 report
#=======================================================================
def clean_902():
	"""
	Clean 902 report
	"""
	with open(datadir + 'cat.csv',"rb") as infile, open('./temp/902_out.csv','wb+') as outfile:
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
						print(msg)
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
						print(msg)
						sub_7 = ''.join(row)
					c.close()
					
				#===================
				# 902$b type of cataloging (chosen manually by cataloger)
				#===================
				if (sub_b != '' and sub_b not in ('b','c','l','m','o','r','s','x','z')) or (sub_b == ''):
					# TODO: refine this: checking 040 and 035
					c = db.cursor()
					toc = '' # type of cataloging
					SQL = """SELECT princetondb.GETALLBIBTAG(BIB_TEXT.BIB_ID, '040',2) as f040, princetondb.GETALLBIBTAG(BIB_TEXT.BIB_ID, '035',2) as f035 FROM BIB_TEXT 
					WHERE BIB_ID = '%s'""" % bbid
					c.execute(SQL)
					for row in c:
						f040 = ''.join(row)
						print(f040)
						if '$aDLC' in f040 and '$cPUL' in f040: 
							toc = 'l'
						elif f040.count('$d') > 1 or '(OCoLC)' in f040:
							toc = 'm'
						elif '$aNjP' in f040:
							toc = 'o'
						msg = '%s,902$b,%s,%s' % (bbid,sub_b,toc)
						change_logger.info(msg)
						print(msg)
							
							
						sub_b = toc
					c.close()
				
				#===================
				# 902$a operator id
				#===================
				# check against operators table => vger history
				if (opid != '' and opid not in (operators)) or (opid == ''): # this covers numbers and other random entries, and blank opids
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
						msg = '%s,902$a,%s,%s' % (bbid,opid,''.join(row))
						change_logger.info(msg)
						print(msg)
						opid = ''.join(row)
					c.close()
				
				#===================
				# 902$d 
				#===================
				# chosen by cataloger -- how to check? 007? These are rarely if ever blank. Most common value is "v"
				if sub_d == '' or sub_d not in ['a','c','d','e','f','g','l','m','r','s','t','v','w']:
					msg = '%s,902$d,%s,%s' % (bbid,sub_d,'v')
					change_logger.info(msg)
					print(msg)
					sub_d = "v"
				
				#===================
				# 902$e
				#===================
				# regex to cut off all but first 8 chars (if numeric)
				if sub_e != '' and not re.match('^\d{8}$',sub_e):
					msg = '%s,902$e,%s,%s' % (bbid,sub_e,sub_e[0:8])
					change_logger.info(msg)
					print(msg)
					sub_e = sub_e[0:8]
					
				#===================
				# 902$f entered manually
				#===================			
				# numbers only, if no number == 1
				non_num = re.compile(r'[^\d]+')
				if sub_f == '':
					msg = '%s,902$f,%s,%s' % (bbid,sub_f,'1')
					sub_f = 1 
					change_logger.info(msg)
					print(msg)
				elif not re.match(r'^[\d]+$', sub_f):
					msg = '%s,902$f,%s,%s' % (bbid,sub_f,non_num.sub('',sub_f))
					sub_f = non_num.sub('',sub_f)
					change_logger.info(msg)
					print(msg)
					
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
					msg = '%s, %s, $g%s $b%s, $g%s $b%s' % (bbid,opid,sub_g,sub_b,'?','m')
					change_logger.info(msg)
					print(msg)
					sub_g = '?'
					if sub_b == 'o':
						sub_b = 'm'
					
				newline = bbid, opid, sub_b, sub_6, sub_7, sub_d, sub_e, sub_f, sub_g, sub_s 
				writer.writerow(newline)
	msg = '902 report is clean!'
	run_logger.info(msg)
	print(msg)
				
#=======================================================================
# clean the 904 report
#=======================================================================
def clean_904():
	"""
	Clean up 904 report.
	"""
	with open(datadir + 'acq.csv',"rb") as infile, open('./temp/904_out.csv','wb+') as outfile:
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
						opid = ''.join(row)
					c.close()
					amsg = '%s,904$a,%s,%s' % (bbid,opid,opid)
					change_logger.info(amsg)
					#print(msg)
				
				#===================
				# 904$b type of receipt
				#===================
				# operators table unit (order* or not)
				bmsg = ''
				if sub_b.strip() not in ['a','d','g','m','o']:
					with open('./lookups/operators.csv','rb') as ops:
						oreader = csv.reader(ops)
						for l in oreader:
							if opid == l[2]:
								unit = l[3] # get unit of operator
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
def process_authorities():
	"""
	Filter last month's authorities report.
	"""
	with open(datadir + 'authorities' + thisrun + '.csv','rb') as auths, open('./temp/auths_out.csv','wb+') as authsout:
		reader = csv.reader(auths)
		writer = csv.writer(authsout)
		#next(reader, None)
		header = ('new order','initials','NACO','updates','SACO','NACO series','name/title','053')
		writer.writerow(header)
		for line in reader:
			order = line[1]
			opid = line[3]
			naco1 = line[4]
			naco2 = line[5]
			naco3 = line[6]
			naco4 = line[7]
			naco5 = line[8]
			update1 = line[9]
			update2 = line[10]
			update3 = line[11]
			update4 = line[12]
			update5 = line[13]
			saco = line[14]
			naco_series = line[15]
			name_ti = line[16]
			f053 = line[17]
			
			# add the NACO and update figures together
			naco = int(naco1) + int(naco2) + int(naco3) + int(naco4) + int(naco5)
			updates = int(update1) + int(update2) + int(update3) + int(update4) + int(update5)
			
			line = order, opid, naco, updates, saco, naco_series, name_ti, f053
			writer.writerow(line)
			
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
		lastid = get_last_row('./temp/903_out.csv')
		src = './temp/903_out.csv'
	lastidchk = raw_input("Last id in "+src+" is " + lastid[0]+'. If this is not correct, enter the last id. ')
	if lastidchk == '':
		lastid = lastid[0]
	else:
		lastid = lastidchk
	lastdate = raw_input("What's the end date (dpk and nb made large entries) yyyy/mm/dd (incl.)? ")
	if not re.match('^\d+$',lastid):
		sys.exit('Id needs to be an integer.')
	
	with open(datadir + 'all903.csv','rb') as f903, open('./temp/903_out.csv','wb+') as f903out:
		reader = csv.reader(f903)
		writer = csv.writer(f903out)
		next(reader, None)
		header = ('ID', 'Initials', 'Sub_B','Sub_C', 'Num_Pieces', 'Note', 'Remote_computer_name', 'User_name', 'Browser_type', 'Timestamp')
		writer.writerow(header)
		for line in reader:
			thisid = line[0]
			timestamp = line[9]
			d = datetime.datetime.strptime(timestamp, '%m/%d/%Y %H:%M:%S').strftime('%Y%m%d')
			if d <= lastdate and int(thisid) > int(lastid[0]):
				writer.writerow(line)
	msg = '903 table has been filtered'
	run_logger.info(msg)
	print(msg)
	
def archive():
	"""
	archive reports; move output tables from temp/ to archive/
	"""
	try:
		for report in ['902','903','904','auths']:
			temp = './temp/' + report + '_out.csv'
			store = archivedir + thisrun + '_' + report + '.csv'
			shutil.copy(temp, store)
	except:
		etype,evalue,etraceback = sys.exc_info()
		print('problem archiving reports ' + evalue)
	
	print('reports archived')

def get_last_row(csv_filename):
	"""
	Grabbed from here: http://stackoverflow.com/questions/20296955/reading-last-row-from-csv-file-python-error
	For getting last row of 903 report.
	"""
    with open(csv_filename, 'rb') as f:
        return deque(csv.reader(f), 1)[0]

	print ', '.join(get_last_row(filename))	
			
if __name__ == "__main__":
	main()
