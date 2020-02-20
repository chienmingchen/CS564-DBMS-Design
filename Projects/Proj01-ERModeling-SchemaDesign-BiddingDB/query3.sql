SELECT count(*) as Auctions
FROM (
    SELECT count(DISTINCT Category.Type) as c
    FROM Category
    GROUP BY Category.ItemID
) countTable
WHERE countTable.c = 4;