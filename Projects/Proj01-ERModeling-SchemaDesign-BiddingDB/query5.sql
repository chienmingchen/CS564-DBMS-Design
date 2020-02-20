SELECT count(DISTINCT Item.SellerID) as Sellers
FROM User, Item
WHERE User.Rating > 1000
AND User.UserID = Item.SellerID;