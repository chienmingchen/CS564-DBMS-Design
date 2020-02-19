drop table if exists User;
drop table if exists Item;
drop table if exists Bid;
drop table if exists Category;
create table User(UserID Text PRIMARY KEY, Rating Real, Location Text, Country Text);
create table Item(
    ItemID Integer PRIMARY KEY,
    Name Text,
    SellerID Text,
    Currently Text,
    Buy_Price Text,
    First_Bid Text,
    Number_of_Bids Text,
    Started Text,
    Ends Text,
    Description Text
);
create table Category(type Text, ItemID Text PRIMARY KEY, FOREIGN KEY(ItemID) REFERENCES Item);
create table Bid(
    ItemID Text,
    BidderID Text, 
    Time Text, 
    Amount Text, 
    PRIMARY KEY(BidderID, Time, Amount),
    FOREIGN KEY(BidderID) REFERENCES User,
    FOREIGN KEY(ItemID) REFERENCES Item
);