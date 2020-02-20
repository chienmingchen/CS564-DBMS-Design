SELECT Item.ItemID as ID 
FROM Item
WHERE Item.Currently = (
    SELECT max(Item.Currently)
    FROM Item
);