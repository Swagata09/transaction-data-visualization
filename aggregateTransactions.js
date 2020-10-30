var sqlite3 = require('sqlite3').verbose();
var db = new sqlite3.Database('swag.db');
var args = process.argv;

db.serialize(function() {
    //Aggregated by user / day: Number of operations and revenue per User per Day

    let sql = ` SELECT  user,
                        count(operation) as No_of_Operations,
                        quantity*unitPrice as Revenue 
                FROM tran   
                WHERE strftime('%Y-%m-%d', datetime / 1000, 'unixepoch') = ? 
                GROUP BY user 
            `
    db.all(sql, [args[2]], (err, rows) => {
        if (err) {
        throw err;
        }
    rows.forEach((row) => {
    console.log(row);

    var stmnt = db.prepare("INSERT INTO Agg_User_Per_Day VALUES(?,?,?)");
    stmnt.run(row['user'],row['No_of_Operations'],row['Revenue'])
    stmnt.finalize();  
    });
}); 
    db.each("SELECT * FROM Agg_User_Per_Day",function(err,row){
        console.log(row);
    });

    // Aggregated by user / hour: Number of operations and revenue per User per Hour
    
    let sql = ` SELECT strftime('%Y-%m-%d %H:00:00', datetime / 1000, 'unixepoch') as datetime,
                       user,
                       count(operation) as No_of_Operations,
                       quantity*unitPrice as Revenue
                FROM tran
                WHERE strftime('%Y-%m-%d', datetime / 1000, 'unixepoch')  = ?
                GROUP BY strftime('%Y-%m-%d %H:00:00', datetime / 1000, 'unixepoch') ,user 
                `
    db.all(sql, [args[2]], (err, rows) => {
        if (err) {
        throw err;
        }
    rows.forEach((row) => {
    console.log(row);

    var stmnt = db.prepare("INSERT INTO Agg_User_Per_Hour VALUES(?,?,?,?)");
    stmnt.run(row['datetime'],row['user'],row['No_of_Operations'],row['Revenue'])
    stmnt.finalize();  
    });
}); 
    db.each("SELECT * FROM Agg_User_Per_Hour",function(err,row){
        console.log(row);
    });
});