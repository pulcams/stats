# stats

N.B. Alma will liberate us from this in 2021

A Python script to automate steps involved in monthly generation of cataloger productivity stats.

After running stats.py, vba and sql scripts in an Access database are used generate traditional reports and to send copies of tables to shares on the network. 
The Access database is 'stats.accdb' on pmg's Windows workstation. 
The default form in 'stats.accdb' has instructions and more details (there are just a few steps). So, basically...

Run stats.py as `python3 stats.py -m yyyymm` (e.g. `python3 stats.py -m 202001`)

After the script has run successfully, the `*_out` files (from ./out, e.g. `902_out.csv`) are imported and processed in 'stats.accdb'.
