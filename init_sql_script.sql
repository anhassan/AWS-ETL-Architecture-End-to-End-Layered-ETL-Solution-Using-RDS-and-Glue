-- Customers Table DDL
CREATE TABLE customers (
    customerid int identity(1,1),
	customername nvarchar(256),
	customerregion nvarchar(256),
	recordtime datetime default getdate()
)

-- Orders Table DDL
CREATE TABLE orders(
	orderid int,
	customerid int,
	amount int,
	recordtime datetime default getdate()
)

-- Inserting Data into Customers Table
INSERT INTO customers(
    customername,
    customerregion) 
    VALUES 
    ('John','England'),
    ('Kelly','United States'),
    ('Katie','England'),
    ('Mike','Australia'),
    ('Bella','England'),
    ('Nathan','Australia');

-- Inserting Data into Orders Table
INSERT INTO orders(
    orderid,
    customerid,
    amount) 
    VALUES 
    (1,1,10),
    (2,2,20),
    (3,3,30),
    (4,4,40),
    (5,5,50),
    (6,6,60),
    (7,1,70),
    (8,5,55),
    (9,6,14),
    (10,3,11);



