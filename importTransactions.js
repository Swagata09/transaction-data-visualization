var sqlite3 = require('sqlite3').verbose();
var db = new sqlite3.Database('swag.db');
var args = process.argv;

const csv = require('csv-parser');
const fs = require('fs');

db.serialize(function() {
    
    fs.createReadStream(args[2])
    .pipe(csv())
    .on('data', (row) => {
    console.log(row['transactionId'],row['user'],row['datetime'],row['operation'],row['quantity'],row['unitPrice']);
   
        var stmnt = db.prepare("INSERT INTO tran VALUES(?,?,?,?,?,?)");
        stmnt.run(row['transactionId'],row['user'],row['datetime'],row['operation'],row['quantity'],row['unitPrice'])
        stmnt.finalize();   
  })
  db.each("SELECT * from tran ",function(err,row){
    console.log(row);
})
 
    .on('end', () => {
    console.log('CSV file successfully processed');
  });
});