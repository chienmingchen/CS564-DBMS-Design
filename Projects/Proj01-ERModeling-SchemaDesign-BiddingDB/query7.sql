SELECT count(DISTINCT Category.type) as categories
FROM Category, Bid
WHERE Bid.Amount > 100
AND Bid.ItemID = Category.ItemID;