var sqlite3 = require('sqlite3').verbose();
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
    
    let sql1 = ` select substr(tranDate,0,11) as tranDate,substr(tranDate,12,2) as hour,user,count(operation) as No_of_Operations_Hr, sum(quantity*unitPrice) as RevenueHr
                from tran where substr(tranDate,0,11) = ?  group by substr(tranDate,0,11),user , substr(tranDate,12,2)  
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