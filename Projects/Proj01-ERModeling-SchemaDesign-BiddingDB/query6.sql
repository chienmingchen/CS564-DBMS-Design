SELECT count(DISTINCT Bid.BidderID) as Users
FROM Bid, Item
WHERE Bid.BidderID = Item.SellerID;