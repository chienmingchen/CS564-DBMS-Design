#!/bin/bash
for i in {0..39}
do
    cat ebay_data/items-${i}.json_Bid.dat >> Bid.dat
    cat ebay_data/items-${i}.json_Category.dat >> Category.dat
    cat ebay_data/items-${i}.json_Items.dat >> Items.dat
    cat ebay_data/items-${i}.json_Users.dat >> Users.dat
done
for i in {0..39}
do
    python3 group8_parser.py ebay_data/items-${i}.json
done
touch Bid.dat
touch Category.dat
touch Items.dat
touch Users.dat
sqlite3 ebay.db < create.sql
sqlite3 ebay.db < load.txt