
import sys
from json import loads
from re import sub
import pandas as pd
import os

columnSeparator = "|"

# Dictionary of months used for date transformation
MONTHS = {'Jan':'01','Feb':'02','Mar':'03','Apr':'04','May':'05','Jun':'06',\
        'Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}

"""
Returns true if a file ends in .json
"""
def isJson(f):
    return len(f) > 5 and f[-5:] == '.json'

"""
Converts month to a number, e.g. 'Dec' to '12'
"""
def transformMonth(mon):
    if mon in MONTHS:
        return MONTHS[mon]
    else:
        return mon

"""
Transforms a timestamp from Mon-DD-YY HH:MM:SS to YYYY-MM-DD HH:MM:SS
"""
def transformDttm(dttm):
    dttm = dttm.strip().split(' ')
    dt = dttm[0].split('-')
    date = '20' + dt[2] + '-'
    date += transformMonth(dt[0]) + '-' + dt[1]
    return date + ' ' + dttm[1]

"""
Transform a dollar value amount from a string like $3,453.23 to XXXXX.xx
"""
def transformDollar(money):
    if money == None or len(money) == 0:
        return money
    return sub(r'[^\d.]', '', money)


def output(dic, filename):
    with open(filename, 'w') as outfile:
        for tup in dic:
            for idx, item in enumerate(tup):
                if type(item) == str:
                    item_split = item.split('"')
                    if len(item_split) > 1:
                        item = ""
                        for i, seg in enumerate(item_split):
                            if seg != "":
                                item += '"'
                                item += seg
                                item += '"'
                outfile.write(str(item))
                if idx != len(tup) - 1:
                    outfile.write("|")
            outfile.write("\n")

"""
Parses a single json file. Currently, there's a loop that iterates over each
item in the data set. Your job is to extend this functionality to create all
of the necessary SQL tables for your database.
"""
def parseJson(json_file):

    f = open(json_file, "r")
    rawData = loads(f.read())['Items']
    Items = []
    Users = []
    Bids = []
    Category = []

    for item in rawData:
        
        """
        abstract Items entity set 
        """
        ItemID = int(item['ItemID'])
        Name = item['Name']
        SellerID = item['Seller']['UserID']
        Currently = transformDollar(item['Currently'])
        First_Bid = transformDollar(item['First_Bid'])
        Number_of_Bids = item['Number_of_Bids'] #this might not be needed
        Started = transformDttm(item['Started'])
        Ends = transformDttm(item['Ends'])
        Description = item['Description']
        if 'Buy_Price' in item.keys():
            Buy_Price = transformDollar(item['Buy_Price'])
        else:
            Buy_Price = 'None'
        Items.append((ItemID, Name, SellerID, Currently, Buy_Price, First_Bid, Number_of_Bids, Started, Ends, Description))
        
        
        """
        abstract Person entity set 
        """   
        SellerRating = item['Seller']['Rating']
        SellerLocation  = item['Location']
        SellerCountry = item['Country']        
        Users.append((SellerID, SellerRating, SellerLocation, SellerCountry))
        
        
        """
        abstract Category entity set 
        """
        for type in item['Category']:
            Category.append((type, ItemID))
                
        """
        abstract Bid relation set
        """        
        if item['Bids'] != None:
            for bid in item['Bids']:
                
                """
                store bid information
                """
                BidderID = bid['Bid']['Bidder']['UserID']
                Time = transformDttm(bid['Bid']['Time'])
                Amount = transformDollar(bid['Bid']['Amount'])
                Bids.append((ItemID, BidderID, Time, Amount))
                
                """
                Need to store bidders to the Person entity set
                Not sure if any empty cases, revision might be needed
                """
                
                BidderRating = bid['Bid']['Bidder']['Rating']
                BidderLocation = bid['Bid']['Bidder'].setdefault('Location','None')
                BidderCountry = bid['Bid']['Bidder'].setdefault('Country','None')
          
                Users.append((BidderID, BidderRating, BidderLocation, BidderCountry))
   
    output(Items, json_file + '_Items.dat')
    output(Category, json_file + '_Category.dat')
    output(Users, json_file + '_Users.dat')
    output(Bids, json_file + '_Bid.dat')

"""
Loops through each json files provided on the command line and passes each file
to the parser
"""
def main(argv):
    if len(argv) < 2:
        print >> sys.stderr, 'Usage: python skeleton_json_parser.py <path to json files>'
        sys.exit(1)
    # loops over all .json files in the argument
    for f in argv[1:]:
        if isJson(f):
            parseJson(f)
            print (("Success parsing " + f))

if __name__ == '__main__':
    main(sys.argv)

