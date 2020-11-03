var sqlite3 = require('sqlite3').verbose();
var db = new sqlite3.Database('hexonet.db');

db.serialize(function() {
    db.run("CREATE TABLE tran (transactionId INT PRIMARY KEY, user TEXT, datetime NUMERIC, operation TEXT, quantity INT, unitPrice NUMERIC,tranDate TEXT,fileName TEXT)");

    db.run("CREATE TABLE Agg_User_Per_Day (date TEXT,user TEXT, No_of_Operations INTEGER, Revenue NUMERIC)");

    db.run("CREATE TABLE Agg_User_Per_Hour (date TEXT,hour TEXT,user TEXT, No_of_Operations INTEGER, Revenue NUMERIC)");
});