# Import transaction data and aggregate it
This repository will demonstrate how to connect SQLite database with node.js application to import and aggregate data


**Node.js | SQLite | Python | Power BI**

## Problem Statement
Create scripts that will process the CSV files, insert data into table and use that data in next script for performing various aggregations on it.
The columns in the CSV files are as follow:
- “transactionId”, // a unique transaction identifier
- “user”, // a unique user identifier
- “datetime”, // a timestamp in ms (javascript format)
- “operation”, // a description of the transaction being charged
- “quantity”,
- “unitPrice” // the revenue of the transaction is quantity * unit_price

We have use SQLite as a databse so the test is self-contained.

## Expected Result
###### Part 1: Create an import script
In the first part, we will create a script that imports the transactions of the
CSV files into an SQL table.
- The script should accept one parameter (the CSV filename). 
For example: node importTransactions.js transactions_2020-08-01.csv
- It should put the information into the DB, in a table that collects all transactions.
- All data from CSVs imported by the script will be placed in this same table.
- It should handle duplicate rows (i.e. same CSV imported twice by mistake or
update existing data by processing a new file).

###### Part 2: Create an aggregation script
In the second part, we will create a script that reads from the SQL table from the previous part and aggregates data into two new DB tables that allow to get the
following information:
- Aggregated by user / day: Number of operations and revenue per User per Day
- Aggregated by user / hour: Number of operations and revenue per User per Hour
- Running the script should look like :
node aggregateTransactions.js 2020-08-01

## Getting Started with NodeJS

###### Installation
`npm install sqlite3`

###### Step 1:
**Create tableCreation.js file in node.js** <br />

First, import the sqlite3 module: <br />

`var sqlite3 = require('sqlite3').verbose();` <br />

Notice that the execution mode is set to verbose to produce long stack traces. <br />

Second, create a Database object: <br />

`var db = new sqlite3.Database('swag.db');` <br />

The sqlite3.Database() returns a Database object and opens the database connection automatically. <br />

Then, create all the required tables in this script **tableCreation.js**. <br />

```var sqlite3 = require('sqlite3').verbose();
var db = new sqlite3.Database('hexonet.db');

db.serialize(function() {
    db.run("CREATE TABLE tran (transactionId INT PRIMARY KEY, user TEXT, datetime NUMERIC, operation TEXT, quantity INT, unitPrice NUMERIC,tranDate TEXT,fileName TEXT)");

    db.run("CREATE TABLE Agg_User_Per_Day (date TEXT,user TEXT, No_of_Operations INTEGER, Revenue NUMERIC)");

    db.run("CREATE TABLE Agg_User_Per_Hour (date TEXT,hour TEXT,user TEXT, No_of_Operations INTEGER, Revenue NUMERIC)");
});
```

###### Step 2: 
**Create importTransactions.js**

This script **importTransactions.js** will include the logic for fetching the records from given CSV files and store it into first table i.e. **tran** table. All data from CSVs imported by the script will be placed in this same table.It will handle the duplicate rows as well. <br />

```
var sqlite3 = require('sqlite3').verbose();
var db = new sqlite3.Database('hexonet.db');
var dateFormat = require('dateformat');
var args = process.argv;

const csv = require('csv-parser');
const fs = require('fs');

db.serialize(function() {
    
    fs.createReadStream(args[2])
    .pipe(csv())
    .on('data', (row) => {
  
    var n = Number(row['datetime'])
    var t1 = new Date(n)
    var tranDate = t1.toISOString().replace(/T/, ' ').replace(/\..+/, '') 
    
    transactionId = row['transactionId']
    user = row['user']
    datetime = row['datetime']
    operation = row['operation']
    quantity = row['quantity']
    unitPrice = row['unitPrice']
    fileName = args[2]

    console.log(row['transactionId'],row['user'],row['datetime'],row['operation'],row['quantity'],row['unitPrice'],tranDate,fileName);

      let sql = `SELECT COUNT(*) as cnt FROM tran WHERE transactionId = ?`
      db.all(sql, [row['transactionId']], (err, rows) => {
        if (err) {
        throw err;
        }
    rows.forEach((row) => {
    console.log(row['cnt']); 
    inputData = [user,datetime,operation,quantity,unitPrice,tranDate,fileName,transactionId]
    console.log(inputData)

      if (row['cnt'] == '1') {
          
          var stmnt = db.prepare("UPDATE tran SET user = ?, datetime = ?, operation = ?, quantity=?, unitPrice = ?,tranDate = ?, fileName = ? WHERE transactionId = ?");
          stmnt.run(inputData)
          stmnt.finalize();     
      }
      else{
            var stmnt = db.prepare("INSERT INTO tran VALUES(?,?,?,?,?,?,?,?)");
            stmnt.run(transactionId,user,datetime,operation,quantity,unitPrice,tranDate,fileName)
            stmnt.finalize();
      }   

    });
}); 
    
  db.each("SELECT count(*) from tran ",function(err,row){
    console.log(row);
})
 
    .on('end', () => {
    console.log('CSV file successfully processed');
  });
});

});
```

Let's run the program as: <br />

`node importTransactions.js transactions_2020-08-01.csv`  <br/>

###### Step 3: 
**Create aggregateTransactions.js**

This script **aggregateTransactions.js** will read the data from **tran** table created in previous step and aggregate data into two new tables that gives following information: <br />
- Aggregated by user / day: Number of operations and revenue per User per Day 
- Aggregated by user / hour: Number of operations and revenue per User per Hour

```var sqlite3 = require('sqlite3').verbose();
var db = new sqlite3.Database('hexonet.db');
var args = process.argv;

db.serialize(function() {
    //Aggregated by user / day: Number of operations and revenue per User per Day

    let sql = ` select substr(tranDate,0,11) as tranDate,user,count(operation) as No_of_Operations ,sum(quantity*unitPrice) as Revenue
                from tran where substr(tranDate,0,11) = ? group by substr(tranDate,0,11),user
            `

    db.all(sql, [args[2]], (err, rows) => {
        if (err) {
        throw err;
        }
    rows.forEach((row) => {
    console.log(row);

    var stmnt = db.prepare("INSERT INTO Agg_User_Per_Day VALUES(?,?,?,?)");
    stmnt.run(row['tranDate'],row['user'],row['No_of_Operations'],row['Revenue'])
    stmnt.finalize();  
    });
}); 
    db.each("SELECT * FROM Agg_User_Per_Day",function(err,row){
    console.log(row);
    });

    // Aggregated by user / hour: Number of operations and revenue per User per Hour
    
    let sql1 = ` select substr(tranDate,0,11) as tranDate,substr(tranDate,12,2) as hour,user,count(operation) as No_of_Operations_Hr, sum(quantity*unitPrice) as                          RevenueHr from tran where substr(tranDate,0,11) = ?  group by substr(tranDate,0,11),user , substr(tranDate,12,2)  
                `
    db.all(sql1, [args[2]], (err, rows) => {
        if (err) {
        throw err;
        }
    rows.forEach((row) => {
    console.log(row); 

    var stmnt = db.prepare("INSERT INTO Agg_User_Per_Hour VALUES(?,?,?,?,?)");
    stmnt.run(row['tranDate'],row['hour'],row['user'],row['No_of_Operations_Hr'],row['RevenueHr'])
    stmnt.finalize();  
    });
}); 
    db.each("SELECT * FROM Agg_User_Per_Hour",function(err,row){
       console.log(row);
    }); 
});
```

Execute script on the command line: </br>

`node aggregateTransactions.js 2020-08-01`

## Possible Solutions

## 1.NodeJS

Following are the links for scripts of NodeJS: <br />
[tableCreation.js](https://github.com/Swagata09/transaction-data-visualization/blob/main/tableCreation.js)  <br />
[importTransaction.js](https://github.com/Swagata09/transaction-data-visualization/blob/main/importTransactions.js) <br />
[aggregateTransactions.js](https://github.com/Swagata09/transaction-data-visualization/blob/main/aggregateTransactions.js) <br />

## 2.Python
Here is the link for Jupyter notebook in which alternate python solution has been enclosed. <br />
[Python Code](https://github.com/Swagata09/transaction-data-visualization/blob/main/Python_Code.ipynb)

## 3.Microsoft Power BI
I have visualize this report in Microsoft Power BI to represent the information in more user friendly way. A non-technical user can also read this report and understand the insights of the data. <br />
This is the link for Power BI report <br />
[Power BI Report](https://github.com/Swagata09/transaction-data-visualization/blob/main/PowerBI_Desktop_Report.pbix)  <br />

Attaching some screenshots for better understanding <br />

The final report will look like this.<br>

![Final Report](https://github.com/Swagata09/transaction-data-visualization/blob/main/screenshots-powerBI-report/final_report.PNG)  <br />

Visualization for 'Number of operations and revenue per User per Day': <br />

![Number of operations and revenue per User per Day](https://github.com/Swagata09/transaction-data-visualization/blob/main/screenshots-powerBI-report/no_of_operations_and_revenue_per_User_per_Day.PNG) <br />

Visualization for 'Number of operations and revenue per User per Hour' <br />

![Number of operations and revenue per User per Hour](https://github.com/Swagata09/transaction-data-visualization/blob/main/screenshots-powerBI-report/no_of%20_perations_and_revenue_per_User_per_Hour.PNG)  <br />

Expanding this report to see per user per hour activity <br />

![Number of operations and revenue per User per Hour Expanded](https://github.com/Swagata09/transaction-data-visualization/blob/main/screenshots-powerBI-report/no_of%20_perations_and_revenue_per_User_per_Hour_expanded.PNG)




## Possible Flaws in this workflow:

1. Further, if we want to prevent same file to get insert multiple times in the database, then we can store the filename and it's upload date-time in separate column and put validation on it.
2. We can utilize available data in more detailed format and generate various reports like:
- Total revenue by each operation
- No.of transactions per day

## Conclusion:

We can analyse this data using various tools like node JS, Python, Tableau, Power BI etc. Alternate **Python code** for this problem has been uploaded in this repository.We can visualize this data in Tableau or Power BI to make it easy to understand and presentale.










