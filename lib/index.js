// BLUEPRINT
var DynamoDB = require("aws-sdk").DynamoDB;
var Endpoint = require("aws-sdk").Endpoint;

// BATCH JOBS
var events = require("events");
var wrap = require("./transfer");

// AVAILABLE ENDPOINTS
var west = new DynamoDB({apiVersion: '2014-04-24'});
west.endpoint = "https://dynamodb.eu-west-1.amazonaws.com";
module.exports.ireland = west;

var central = new DynamoDB({apiVersion: '2014-04-24'});
central.endpoint = "https://dynamodb.eu-central-1.amazonaws.com";
module.exports.central = central;

var local = new DynamoDB({apiVersion: '2014-04-24'});
local.endpoint = "http://localhost:8000";
module.exports.local = local;

var schemas;
var tables;
var transfer;

// SETUP
module.exports.setTables = function( tables ) {
  tables = tables;
}
module.exports.setSchemas = function( schemas ) {
  schemas = schemas;
  transfer = wrap( schemas );
};

// EXTRA FUNCTIONS
module.exports.listTables = function( db, params, callback )
{
  /*
    var params = {
      Limit: 0,
      ExclusiveStartTableName: ""
    };

    @return data = {
      TableNames: ""
    }
  */

  db.listTables(  params, callback || fallback);
};

module.exports.describeTable = function( db, table, callback ) {

    var params = {
      TableName: table
    };

  /*
    @return data = {
      "Table": {
        "AttributeDefinitions":[
          {"AttributeName":"IAID","AttributeType":"S"}
          ],
        "TableName":"Matrix",
        "KeySchema":[
          {"AttributeName":"IAID","KeyType":"HASH"}
        ],
        "TableStatus":"ACTIVE",
        "CreationDateTime":"2014-08-19T16:18:12.755Z",
        "ProvisionedThroughput":{
          "NumberOfDecreasesToday":0,
          "ReadCapacityUnits":1,
          "WriteCapacityUnits":1
        },
        "TableSizeBytes":0,
        "ItemCount":0
      }
    }
  */
  db.describeTable( params, callback || fallback);
};

module.exports.createTable = function( db, table, callback ) {

  /*
    @return data = {

    }
  */
  var params = tables[table];

  db.createTable( params, callback || fallback);
};

module.exports.updateTable = function( db, params, callback ) {

  /*
    var params = {
      TableName: '',
      ProvisionedThroughput: ''
    };

    @return data = {
      TableDescription:{
      }
    }
  */
  db.updateTable( params, callback || fallback);
};

module.exports.deleteTable = function(db, table, callback ) {

  /*
    http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#deleteTable-property

    @return data = {
    }
  */
  var params = {
    TableName: table
  };

  db.deleteTable( params, callback || fallback);
};

module.exports.hasTable = function( db, table, callback )
{
  var params = {
    TableName: table
  };

  function repeat() {
    db.describeTable( params, function(err,data) {
      console.log(data);
      if( err ) callback(err);
      if(data && data.Table.TableStatus != "ACTIVE") setTimeout(repeat, 10000);
      if(data && data.Table.TableStatus == "ACTIVE") callback( null, data );
    });
  }

  setTimeout( repeat, 2000 );
};

module.exports.putItem = function( db, params, callback ) {

  /*
    Creates a new item, or replaces an old item with a new item
    (including all the attributes). If an item already exists in the
    specified table with the same primary key, the new item completely
    replaces the existing item.

    var params = {
      TableName:"",
      Item:{},
      Expected: {
        Exists: (true|false),
        Value: { type: value },
      },
      ReturnConsumedCapacity: ('INDEXES | TOTAL | NONE'),
      ReturnItemCollectionMetrics: ('SIZE | NONE')
      ReturnValues: ('NONE | ALL_OLD | UPDATED_OLD | ALL_NEW | UPDATED_NEW')
    }

   @return data = {
      Attributes:{}
    }
  */
   var table = params.TableName;

   params.Item = transfer.to( table, params.Item );

   db.putItem( params, function( err, data ){

      if( data ) data.Attributes = transfer.from( table, data.Attributes );

      callback( err, data );
   });
};

module.exports.getItem = function( db, params, callback )
{
  /*
    var params = {
      TableName:"",
      Key: {},
      AttributesToGet: [],
      ConsistentRead: (true|false)
    }

    @return data = {
      Item: {}
    }
  */
  var table = params.TableName;

  params.Key = transfer.to( table, params.Key );

  db.getItem( params, function( err, data ){

      if( data ) data.Item = transfer.from( table, data.Item );

      callback( err, data );
   });
};

module.exports.updateItem = function( db, params, callback ) {

  /*
    Updates an item with the supplied update orders.

    var params = {
      TableName: "",
      Key: {},
      AttributeUpdates {
        // ADD only on number and sets
        'key': { Action:('PUT'|'ADD'|'DELETE'), Value: { "N":"1" }
        },
        Expected: {
          Exists: (true|false)
          Value: { type: value }
        }
      }
    }

    @return data = {
      ReturnConsumedCapacity: 'INDEXES | TOTAL | NONE'
      ReturnItemCollectionMetrics: 'SIZE | NONE'
      ReturnValues: 'NONE | ALL_OLD | UPDATED_OLD | ALL_NEW | UPDATED_NEW'
    }
  */
  var table = params.TableName;

  params.Key = transfer.to( table, params.Key );

  db.updateItem( params, function( err, data ){

    if( data ) data.Attributes = transfer.from( table, data.Attributes );

    callback( err, data );
  });
};

module.exports.deleteItem = function( db, params, callback ) {
  /*
    Delete an item

    var params = {
      TableName:'',
      Key: {},
      Expected: {
        'key': { Exists: (true|false), Value: { 'type':value } }
      }
    }

    @return data = {
      ReturnConsumedCapacity: ('INDEXES|TOTAL|NONE'),
      ReturnItemCollectionMetrics: ('SIZE|NONE'),
      ReturnValues: ('NONE|ALL_OLD|UPDATED_OLD|ALL_NEW|UPDATED_NEW')
    }
  */
  var table = params.TableName;

  params.Key = transfer.to( table, params.Key );

  db.deleteItem( params, function( err, data ){

    if( data ) data.Attributes = transfer.from( table, data.Attributes );

    callback( err, data );
  });
};

module.exports.batchWriteItem = function( db, param, callback ) {

  // Array with bloated requests
  var batch = [];

  Object.keys( param.RequestItems ).forEach( function( table ) {
    param.RequestItems[table].forEach( function( request ) {
      if( request.DeleteRequest )
      batch.push( { Table: table, DeleteRequest: { Key: transfer.to( table, request.DeleteRequest.Key ) } } );
      if( request.PutRequest )
      batch.push( { Table: table, PutRequest: { Item: transfer.to( table, request.PutRequest.Item ) } } );
    });
  });

  db.batchWriteItem( params, callback );
};

module.exports.batchWriteStream = function( db, param ) {

  // Array with bloated requests
  var batch = [];

  Object.keys( param.RequestItems ).forEach( function( table ) {
    param.RequestItems[table].forEach( function( request ) {
      if( request.DeleteRequest )
      batch.push( { Table: table, DeleteRequest: { Key: transfer.to( table, request.DeleteRequest.Key ) } } );
      if( request.PutRequest )
      batch.push( { Table: table, PutRequest: { Item: transfer.to( table, request.PutRequest.Item ) } } );
    });
  });

  var total = batch.length;

  // Return max 25 requests
  function next() {
    var RequestItems = {};

    for( i = 0; i < 25; i++ ) {
      var request = batch.pop();
      if( !request ) break;
      var table = request.Table;
      if( !RequestItems[table] ) RequestItems[table] = [];
      // Remove helper attributes from actual request
      delete request.Table;
      RequestItems[table].push( request );
    }
    return RequestItems;
  }


  function payload() {

    events.EventEmitter.call( this );
    var that = this;

    // Recursiv Callback
    function callback( err, data ) {

      if( err) that.emit("error", err );

      // NOT DONE BECAUSE OF API RESTRICTIONS
      if( !err && Object.keys( data.UnprocessedItems ).length > 0 ) {
        param.RequestItems = data.UnprocessedItems;
        db.batchWriteItem( param, callback );
      }
      else if( !err && batch.length > 0 ) {
      // PROGRESS
        that.emit("data", { Current: total - batch.length, Total: total });
        param.RequestItems = next();
        db.batchWriteItem( param, callback );
      }
      else {
        // DONE
        if( data ) that.emit("done", {Current: total - batch.length, Total: total });
      }

    }

    param.RequestItems = next();
    // START
    db.batchWriteItem( param, callback );
  }

  require("util").inherits( payload, events.EventEmitter );

  return new payload();
};

module.exports.batchGetItem = function(db, param, callback ) {
  // Array with bloated requests
  var batch = [];

  Object.keys( param.RequestItems ).forEach( function( table ) {
    param.RequestItems[table].Keys.forEach( function( key ) {
      batch.push( { Table: table, Key:  transfer.to( table, key ) } );
    });
  });

  db.batchGetItem( param, function(err,data) {

    if( data ) {
        var tableMap = data.Responses;
        for( var table in tableMap ) {
          var temp = tableMap[ table ];
          tableMap[ table ] = temp.map( function( item ) {
            current++;
            return transfer.from( table, item );
          });
        }
        data.Responses = tableMap;
    }

    callback( err, data );
  });
};

module.exports.batchGetStream = function(db, param, cb ) {
  /*
  @return {
      Items: {
        'TableName': [ {items} ]
      }
    }
  */
  // Array with bloated requests
  var batch = [];

  Object.keys( param.RequestItems ).forEach( function( table ) {
    param.RequestItems[table].Keys.forEach( function( key ) {
      batch.push( { Table: table, Key:  transfer.to( table, key ) } );
    });
  });

  var total = batch.length;

  // Return max 25 requests
  function next() {
    var RequestItems = {};

    for( i = 0; i < 100; i++ ) {
      var item = batch.pop();
      if( !item ) break;
      var table = item.Table;
      if( !RequestItems[table] ) RequestItems[table] = { Keys:[] };
      RequestItems[table].Keys.push( item.Key );
    }
    return RequestItems;
  }

  var current = 0;

  function payload() {

    events.EventEmitter.call( this );
    var that = this;

    // Recursiv Callback
    function callback( err, data ) {
      // Unbloat Results
      if( data ) {
        var tableMap = data.Responses;
        for( var table in tableMap ) {
          var temp = tableMap[ table ];
          tableMap[ table ] = temp.map( function( item ) {
            current++;
            return transfer.from( table, item );
          });
        }
        data.Responses = tableMap;
      }
      // Still items to retrieve
      if( !err && Object.keys( data.UnprocessedKeys ).length > 0 ) {
        that.emit("data", { Items: data.Responses, Current:current, Total:total });
        param.RequestItems = data.UnprocessedKeys;
        db.batchGetItem( param, callback );
      }
      else if( !err && batch.length > 0 ) {
        that.emit("data", { Items: data.Responses, Current:current ,Total:total });
        param.RequestItems = next();
        db.batchGetItem( param, callback );
      }
      else { 
        if(data)
        {
          // FIRST RESPONSE IS ALREADY COMPLETE
          if( data.Responses ) that.emit("data", { Items: data.Responses, Current:current ,Total:total });

          that.emit("done", { Current:current ,Total:total });
        }
        else that.emit("error", err );
      }
    }

    param.RequestItems = next();
    // START
    db.batchGetItem( param, callback );
  }

  require("util").inherits( payload, events.EventEmitter );

  return new payload();
};

module.exports.query = function( db, params, callback ) {

  var table = params.TableName;

  db.query( params, function( err, data) {

    if( data )
    {
      if(data.Items)
      data.Items = data.Items.map( function( item ){
        return transfer.from( table, item );
      });
    }

    callback( err,data );
  });

};

module.exports.queryStream = function( db, params, callback ) {
  /*

    var params = {
      TableName:'',
      Conditions:{
        'key': {
          ComparisonOperator: ('EQ|NE|IN|LE|LT|GE|GT|BETWEEN|NOT_NULL|NULL|CONTAINS|NOT_CONTAINS|BEGINS_WITH'),
          AttributeValueList [ { 'type':value } ]
        }
      },
      Options: {
        AttributesToGet:[],
        ConsistentRead:(true|false),
        ExclusiveStartKey: {
          'key': { 'type':value }
        },
        IndexName:'',
        Limit:0,
        ReturnConsumedCapacity: ('INDEXES|TOTAL|NONE'),
        ScanIndexForward: (true|false),
        Select:('ALL_ATTRIBUTES|ALL_PROJECTED_ATTRIBUTES|SPECIFIC_ATTRIBUTES|COUNT'),
      }
    };
   */
  var table = params.TableName;

  function payload() {

    events.EventEmitter.call( this );
    var that = this;

    function callback( err, data ) {

      if( err ) that.emit( "error", err );

      if( data )
      {
        if(data.Items)
        data.Items = data.Items.map( function( item ){
          return transfer.from( table, item );
        });

        that.emit( "data", data );

        if( data.LastEvaluatedKey ) {
          params.ExclusiveStartKey = data.LastEvaluatedKey;
          db.query( params, callback );
        }
      }

      if( data && !data.LastEvaluatedKey ) that.emit("done");
    }

    db.query( params, callback );
  }

  require("util").inherits( payload, events.EventEmitter );

  return new payload();
};

module.exports.scan = function( db, params, callback ) {

  var table = params.TableName;

  db.scan( params, function(err,data) {

      if( data )
      {
        if(data.Items)
        data.Items = data.Items.map( function( item ){
          return transfer.from( table, item );
        });
      }

      callback( err, data );
  });

};

module.exports.scanStream = function(db, params ) {
  /*
    returns one or more items and its attributes by performing a full scan of a table.

     var params = {
       TableName:'',
       Conditions: {
        'key': {
          // required
          ComparisonOperator: ('EQ|NE|IN|LE|LT|GE|GT|BETWEEN|NOT_NULL|NULL|CONTAINS|NOT_CONTAINS|BEGINS_WITH'),
          AttributeValueList:[ { 'type':value } ]
        },
        Options: {
          AttributesToGet: [],
          ExclusiveStartKey: { 'key': { 'type':value } },
          Limit:0,
          ReturnConsumedCapacity: ('INDEXES|TOTAL|NONE'),
          Segment: 0,
          Select:('ALL_ATTRIBUTES|ALL_PROJECTED_ATTRIBUTES|SPECIFIC_ATTRIBUTES|COUNT'),
          TotalSegments: 0
        }
       }
     };
   */
  var table = params.TableName;
  var count = 0;

  function payload() {

    events.EventEmitter.call( this );
    var that = this;

    function callback( err, data ) {

      if( err ) that.emit( "error", err );

      // Count
      // Items
      if( data )
      {
        if( data.Items )
        data.Items = data.Items.map( function( item ){
          return transfer.from( table, item );
        });

        that.emit( "data", data );

        if( data.LastEvaluatedKey ) {
          params.ExclusiveStartKey = data.LastEvaluatedKey;
          db.scan( params, callback );
        }
      }

      if( data && !data.LastEvaluatedKey ) that.emit("done");
    }

    db.scan( params, callback );
  }

  require("util").inherits( payload, events.EventEmitter );

  return new payload();
};

function fallback(err, data) {
  if(err) console.log(err, err.name, err.message);
  else console.log( data );
}
