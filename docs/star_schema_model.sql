-- DIMENSIONS

CREATE TABLE Dim_Customer (
    Customer_SK SERIAL PRIMARY KEY,
    Customer_BK VARCHAR,
    First_Name VARCHAR,
    Last_Name VARCHAR,
    Email VARCHAR,
    Phone VARCHAR,
    Address VARCHAR,
    Is_Current BOOLEAN,
    Effective_From TIMESTAMP,
    Effective_To TIMESTAMP
);

CREATE TABLE Dim_Product (
    Product_SK SERIAL PRIMARY KEY,
    Product_BK VARCHAR,
    Product_Name VARCHAR,
    Category VARCHAR,
    Price DECIMAL(10,2)
);

CREATE TABLE Dim_Date (
    Date_SK SERIAL PRIMARY KEY,
    Date DATE,
    Day INT,
    Month INT,
    Year INT,
    Weekday VARCHAR
);

-- FACT TABLE

CREATE TABLE Fact_Order (
    Order_SK SERIAL PRIMARY KEY,
    Order_BK VARCHAR,
    Customer_SK INT,
    Product_SK INT,
    Order_Date DATE,
    Quantity INT,
    Unit_Price DECIMAL(10,2),
    Total_Amount DECIMAL(10,2)
);