Stats
=====

Python script to automate steps involved in monthly generation of cataloger productivity stats.

After running stats.py, scripts in an Access database are used generate familiar-looking reports and to send copies of tables to shares on the network. The Access database is 'stats.accdb' on pmg's Windows machine. The default form in 'stats.accdb' has instructions and more details (there are just a few steps). So, basically...

Run stats.py as `python stats.py -m yyyymm` (e.g. `python stats.py -m 201602`)

After the script has run successfully, the `_out` files (from ./out, e.g. `902_out.csv`) are sent to the W7 machine and imported into 'stats.accdb'.

Needs
=====
mdbtools `sudo apt-get install mdbtools`

TODO
====
* send tables to tsserver? 
* document mounting shares with noserverino,nounix
* generate reports (jinja?)
* email / post (where?)
* sync changes to operators table (master copy on lib-tsserver)
* how-tos on tsserver -- legacy and current

