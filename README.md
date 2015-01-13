Stats
=====

Python script to automate steps involved in monthly generation of cataloger productivity stats.

At this stage, scripts in an Access database are also part of the workflow both before and after running stats.py. The Access database is stats.accdb on pmg's Windows machine. The default form in this database has instructions (just a few steps). 

Before running stats.py the following tables (as .csv files) need to be in ./data/:
(1) Results.csv from \\lib-terminal\catalog\fpdb (renamed as 'all903.csv')
(2) latest 90x report from \\lib-tsserver\CaMS\for Lena\90x stats.mdb (renamed as 'authoritiesyyyymm.csv')

Once these have been gathered, run stats.py as `python stats.py -m yyyymm` (e.g. `python stats.py -m 201504`)

After the script has run successfully, the _out files (in ./temp) are imported into stats.accdb (on the Windows machine).

TODO
====
* export from / import to Access databases from Python script (eliminate need for stats.accdb)
* generate reports from Python (jinja?)
* email / post directly from script
* centralize lookup tables outside of Access (e.g. put master copies on lib-tsserver and inform other users)

