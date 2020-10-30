var sqlite3 = require('sqlite3').verbose();
var db = new sqlite3.Database('swag.db');

db.serialize(function() {
    db.run("CREATE TABLE tran (transactionId INT, user TEXT, datetime NUMERIC, operation TEXT, quantity INT, unitPrice NUMERIC)");

    db.run("CREATE TABLE Agg_User_Per_Day (user TEXT, No_of_Operations INTEGER, Revenue NUMERIC)");

    db.run("CREATE TABLE Agg_User_Per_Hour (Hour TEXT,user TEXT, No_of_Operations INTEGER, Revenue NUMERIC)");
});