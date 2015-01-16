Stats
=====

Python script to automate steps involved in monthly generation of cataloger productivity stats.

At this stage, scripts in an Access database are also part of the workflow after running stats.py, to generate familiar reports and send copies of tables to shares on the network. The Access database is 'stats.accdb' on pmg's Windows machine. The default form in 'stats.accdb' has instructions and more details (there are just a few steps). Basically...

Run stats.py as `python stats.py -m yyyymm` (e.g. `python stats.py -m 201602`)

After the script has run successfully, the _out files (from ./out) are sent to the W7 machine and imported into 'stats.accdb'.

TODO
====
* document mounting with cifs
* generate reports from Python (jinja?)
* email / post directly from script
* centralize lookup tables outside of Access (e.g. put master copies on lib-tsserver and inform other users)
* generate visualizations (individuals over time, turn-around time, etc.)

