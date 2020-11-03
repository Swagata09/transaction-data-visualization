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
    //var n = Number(1596330211174) 
    //console.log(n)
    
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