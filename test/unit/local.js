var assert = require("assert");
var spawn = require('child_process').spawn;
var db = require("../../lib");

var store = db.central;

db.setTables( require("../db/tables") );
db.setSchemas( require("../db/schemas") );

describe("Module dynamodb test local >>", function() {

  var child;
  
  it("Start local db", function( done ) {
  
    child = spawn('java', [
      '-Djava.library.path=./test/db/jar/DynamoDBLocal_lib',
      '-jar',
      './test/db/jar/DynamoDBLocal.jar',
      //'-inMemory',
      '-dbPath','./test/db/data',
      '-port','8000'
    ],{});

    var first = false;
    child.stderr.on('data', function (data) {
      
      if( !first ) {
        process.stdout.write("++ LOCALDB started");
        process.stdout.write(data);

        done();
      }
      first = true;
      
    });
    
    this.timeout( 10000 );
  });
     
  
  it("No tables exit", function( done ) 
  {    
    var params = { Limit:5 };
    
    db.listTables( store, params, function(err, data) {
      assert.equal( data.TableNames.length, 0, "Tables "+data.TableNames.join(" ")+" exist");
      done();
    });
  });
  
  
  it("Stop local db", function( done ) {
  
    
      process.on('exit', function () {
        process.stdout.write("++ LOCALDB stopped");
        done();
      });
    
      child.kill();
    
      this.timeout( 10000 );
  });
  
});



