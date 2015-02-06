var assert = require("assert");

var db = require("../../lib");

var store = db.central;

db.setTables( require("../db/tables") );
db.setSchemas( require("../db/schemas") );

describe("Module dynamodb test central >>", function() {

  it("No tables exit", function( done ) 
  {    
    var params = { Limit:5 };
    
    db.listTables( store, params, function(err, data) {
      assert.equal( data.TableNames.length, 0, "Tables "+data.TableNames.join(" ")+" exist");
      done();
    });
  });
  
});