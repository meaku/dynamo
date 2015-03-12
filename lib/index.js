"use strict";

var when = require("when"),
  nodefn = require("when/node"),
  poll = require("when/poll"),
  _ = require("lodash"),
  bindCallback = nodefn.bindCallback,
  AWSDynamoDB = require("aws-sdk").DynamoDB,
  transform = require("./transform"),
  streams = require("./streams");


function DynamoDb() {
  DynamoDb.prototype.constructor.apply(this, arguments);
}

DynamoDb.prototype.constructor = function (options) {
  this.client = new AWSDynamoDB(options);

  if (options.transform && options.transform === false) {
    this.useTransform(false);
    return;
  }

  this.useTransform(true);
};

/**
 * enable / disable transformation
 * call with true or no argument to enable
 * or with false to disable
 *
 * @param {Boolean=} useTransform
 */
DynamoDb.prototype.useTransform = function (useTransform) {

  if (useTransform === false) {
    this.transform = null;
    return;
  }

  this.transform = transform;
};

/**
 * set tables to be used by created table accessing via "TableName"
 * @param tables
 */
DynamoDb.prototype.setTables = function (tables) {
  this.tables = tables;
};

/**
 *
 * TODO returns tables wrapped in TableNames which is inconvenient? Maybe transform here
 * or add flag for raw mode or improved response mode
 *
 *
 * @param {Object=} params
 * @param {Function=} callback
 * @returns {*}
 */
DynamoDb.prototype.listTables = function (params, callback) {
  return bindCallback(nodefn.call((this.client.listTables).bind(this.client), params), callback);
};

/**
 * describe table
 *
 * @param {String|Object} table
 * @param {Function=} callback
 * @returns {Promise}
 */
DynamoDb.prototype.describeTable = function (table, callback) {

  if (typeof table === "string") {
    table = {
      TableName: table
    };
  }

  return bindCallback(
    nodefn.call((this.client.describeTable).bind(this.client), table),
    callback
  );
};

/**
 * delete all existing tables
 *
 * additional convenience method
 *
 * @param {Function=} callback optional callback
 * @returns {Promise}
 */
DynamoDb.prototype.deleteAllTables = function (callback) {

  var self = this;

  return bindCallback(
    self.listTables()
      .then(function (res) {
        return when.map(res.TableNames, function (table) {
          return self.deleteTable(table);
        });
      }),
    callback
  );
};

/**
 * create table
 *
 * @param {String|Object} table
 * @param {Function=} callback
 * @returns {Promise}
 */
DynamoDb.prototype.createTable = function (table, callback) {

  if (typeof table === "string" && this.tables && this.tables[table]) {
    table = this.tables[table];
  }

  return bindCallback(
    nodefn.call((this.client.createTable).bind(this.client), table),
    callback
  );
};

/**
 * Updates the provisioned throughput for the given table, or manages the global secondary indexes on the table.
 *
 *  var params = {
 *    TableName: '',
 *    ProvisionedThroughput: ''
 *  };
 *
 *  returns data = {
 *    TableDescription:{
 *     }
 *   }
 *
 * @param {Object=} params
 * @param {Function=} callback
 * @returns {Promise}
 */
DynamoDb.prototype.updateTable = function (params, callback) {
  return bindCallback(nodefn.call((this.client.updateTable).bind(this.client), params), callback);
};

/**
 * delete table
 *
 * @param {String|Object} table
 * @param {Function=} callback
 * @returns {Promise}
 */
DynamoDb.prototype.deleteTable = function (table, callback) {

  if (typeof table === "string") {
    table = {
      TableName: table
    };
  }

  return bindCallback(nodefn.call((this.client.deleteTable).bind(this.client), table), callback);
};

/**
 * has table
 *
 * CREATING - The table is being created.
 * UPDATING - The table is being updated.
 * DELETING - The table is being deleted.
 * ACTIVE - The table is ready for use.
 *
 * @param {String|Object} table
 * @param {Function=} callback
 * @returns {Promise}
 */
DynamoDb.prototype.hasTable = function (table, callback) {
  var self = this,
    interval = 1000,
    timeout = 5000;

  if (typeof table === "string") {
    table = {
      TableName: table
    };
  }

  return bindCallback(
    poll(
      function work() {
        return self.describeTable(table);
      },
      interval,
      function condition(data) {
        return data && data.Table.TableStatus === "ACTIVE";
      })
      .timeout(timeout)
      // we resolve with booleans
      .then(function () {
        return true;
      })
      .catch(function (err) {
        //we only catch timeout errors
        if (err.name === "TimeoutError") {
          return false;
        }

        throw err;
      }),
    callback
  );
};

/**
 * ITEM API
 * @type {DynamoDb}
 */

/**
 * Creates a new item, or replaces an old item with a new item
 * (including all the attributes). If an item already exists in the
 * specified table with the same primary key, the new item completely
 * replaces the existing item.
 *
 *  var params = {
 *    TableName:"",
 *    Item:{},
 *
 *    Expected: {
 *      Exists: (true|false),
 *      Value: { type: value },
 *    },
 *    ReturnConsumedCapacity: ('INDEXES | TOTAL | NONE'),
 *    ReturnItemCollectionMetrics: ('SIZE | NONE')
 *    ReturnValues: ('NONE | ALL_OLD | UPDATED_OLD | ALL_NEW | UPDATED_NEW')
 *  }
 *
 * returns data = {
 *     Attributes:{}
 *   }
 *
 * @param params
 * @param callback
 */
DynamoDb.prototype.putItem = function (params, callback) {

  if (this.transform) {
    params.Item = this.transform.to(params.Item);
  }

  return bindCallback(
    nodefn.call((this.client.putItem).bind(this.client), params),
    callback
  );
};

/**
 *
 * //TODO returns result wrapped in { Item: {} }. No so cool?
 *
 * The GetItem operation returns a set of attributes for the item with the given primary key.
 * If there is no matching item, GetItem does not return any data.
 *
 * GetItem provides an eventually consistent read by default.
 * If your application requires a strongly consistent read, set ConsistentRead to true.
 * Although a strongly consistent read might take more time than an eventually consistent read,
 * it always returns the last updated value.
 *
 *  var params = {
 *    TableName:"",
 *    Key: {},
 *    AttributesToGet: [],
 *    ConsistentRead: (true|false)
 *  }
 *
 *  returns data = {
 *    Item: {}
 *  }
 *
 * TODO maybe return ITEM directly instead of nested res.Item
 *
 * @param params
 * @param callback
 * @returns {Promise}
 */
DynamoDb.prototype.getItem = function (params, callback) {

  var self = this;

  if (this.transform) {
    params.Key = self.transform.to(params.Key);
  }

  return bindCallback(
    nodefn.call((this.client.getItem).bind(this.client), params)
      .then(function (res) {

        if (res.Item && self.transform) {
          res.Item = self.transform.from(res.Item);
        }

        return res;
      }),
    callback
  );
};

/**
 * delete an item
 *
 * var params = {
 *   TableName:'',
 *   Key: {},
 *   Expected: {
 *     'key': {
 *       Exists: (true|false),
 *       Value: {
 *         'type':value
 *       }
 *     }
 *   }
 *}
 *
 * returns data = {
 *    ReturnConsumedCapacity: ('INDEXES|TOTAL|NONE'),
 *    ReturnItemCollectionMetrics: ('SIZE|NONE'),
 *    ReturnValues: ('NONE|ALL_OLD|UPDATED_OLD|ALL_NEW|UPDATED_NEW')
 * }
 *
 *
 * @param {Object} params
 * @param {Function=} callback
 * @returns {Promise}
 */
DynamoDb.prototype.deleteItem = function (params, callback) {
  var self = this;

  if (this.transform) {
    params.Key = self.transform.to(params.Key);
  }

  return bindCallback(
    nodefn.call((this.client.deleteItem).bind(this.client), params)
      .then(function (data) {

        if (data && self.transform && data.Attributes) {
          data.Attributes = self.transform.from(data.Attributes);
        }

        return data;
      }),
    callback
  );
};

/**
 * Updates an item with the supplied update orders.
 *
 * var params = {
 *    TableName: "",
 *    Key: {},
 *    AttributeUpdates {
 *    // ADD only on number and sets
 *    'key': { Action:('PUT'|'ADD'|'DELETE'), Value: { "N":"1" }
 *    },
 *    Expected: {
 *    Exists: (true|false)
 *    Value: { type: value }
 *    }
 *    }
 *    }
 *
 * returns data = {
 *    ReturnConsumedCapacity: 'INDEXES | TOTAL | NONE'
 *    ReturnItemCollectionMetrics: 'SIZE | NONE'
 *    ReturnValues: 'NONE | ALL_OLD | UPDATED_OLD | ALL_NEW | UPDATED_NEW'
 *  }
 *
 * @param {Object} params
 * @param {Function=} callback
 * @returns {Promise}
 */
DynamoDb.prototype.updateItem = function (params, callback) {
  var self = this;

  if (this.transform) {
    params.Key = transform.to(params.Key);
  }

  return bindCallback(
    nodefn.call((this.client.updateItem).bind(this.client), params)
      .then(function (data) {

        if (data && self.transform && data.Attributes) {
          data.Attributes = self.transform.from(data.Attributes);
        }

        return data;
      }),
    callback
  );
};

/**
 * Batch Item API
 **/

/**
 * batch write items
 *
 * @param {Object} params
 * @param {Function=} callback
 * @returns {Promise}
 */
DynamoDb.prototype.batchWriteItem = function (params, callback) {

  var self = this,
    batchWriteItem = nodefn.lift(this.client.batchWriteItem.bind(this.client)),
    retryCount = 0;

  if (this.transform) {

    //tables
    Object.keys(params.RequestItems).forEach(function (table) {

      //requests
      params.RequestItems[table].forEach(function (request) {

        if (request.DeleteRequest) {
          request.DeleteRequest.Key = self.transform.to(request.DeleteRequest.Key);
        }

        if (request.PutRequest) {
          request.PutRequest.Item = self.transform.to(request.PutRequest.Item);
        }
      });
    });
  }

  function doBatchWrite(params) {

    if (retryCount++ === 5) {
      throw new Error("Retry limit exceeded");
    }

    return batchWriteItem(params)
      .then(function (res) {

        if (Object.keys(res.UnprocessedItems).length > 0) {
          return doBatchWrite({
            RequestItems: res.UnprocessedItems
          });
        }

        return res;
      });
  }

  return bindCallback(
    doBatchWrite(params),
    callback
  );
};

/**
 * batch get item
 *
 * @param {Object} params
 * @param {Function=} callback
 * @returns {Promise}
 */
DynamoDb.prototype.batchGetItem = function (params, callback) {
  var self = this,
    batchGetItem = nodefn.lift(this.client.batchGetItem.bind(this.client)),
    responses = {},
    retryCount = 0;

  if (this.transform) {

    //tables
    Object.keys(params.RequestItems).forEach(function (table) {

      //keys
      params.RequestItems[table].Keys = params.RequestItems[table].Keys.map(function (key) {
        return self.transform.to(key);
      });
    });
  }

  function doBatchGet(params) {

    if (retryCount++ === 5) {
      throw new Error("Retry limit exceeded");
    }

    return batchGetItem(params)
      .then(function (res) {

        responses = _.merge(responses, res.Responses);

        if (Object.keys(res.UnprocessedKeys).length > 0) {
          return doBatchGet({
            RequestItems: res.UnprocessedKeys
          });
        }

        res.Responses = responses;

        return res;
      });
  }

  return bindCallback(
    doBatchGet(params)
      .then(function (res) {

        if (self.transform && res.Responses) {

          //tables
          Object.keys(res.Responses).forEach(function (table) {
            //items
            res.Responses[table] = res.Responses[table].map(function (item) {
              return self.transform.from(item);
            });
          });
        }

        return res;
      }),
    callback
  );
};

/**
 * QUERY & SCAN API
 **/

/**
 * A Query operation directly accesses items from a table using the table primary key,
 * or from an index using the index key. You must provide a specific hash key value.
 * You can narrow the scope of the query by using comparison operators on the range key value,
 * or on the index key. You can use the ScanIndexForward parameter to get results in forward
 * or reverse order, by range key or by index key.
 *
 * {@link http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#query-property AWS Docs}.
 *
 * @param {Object} params
 * @param {Function} callback
 * @returns {Promise}
 */
DynamoDb.prototype.query = function (params, callback) {
  var self = this;

  return bindCallback(
    nodefn.call((this.client.query).bind(this.client), params)
      .then(function (res) {

        if (res.Items && self.transform) {
          res.Items = res.Items.map(function (item) {
            return self.transform.from(item);
          });
        }

        return res;
      }),
    callback);
};

/**
 * The Scan operation returns one or more items and item attributes by accessing every item in a table
 * or a secondary index. To have DynamoDB return fewer items, you can provide a ScanFilter operation.
 *
 * If the total number of scanned items exceeds the maximum data set size limit of 1 MB,
 * the scan stops and results are returned to the user as a LastEvaluatedKey value to
 * continue the scan in a subsequent operation. The results also include the number of items
 * exceeding the limit. A scan can result in no table data meeting the filter criteria.
 *
 * {@link http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#scan-property AWS Docs}.
 *
 * @param {Object} params
 * @param {Function} callback
 * @returns {Promise}
 */
DynamoDb.prototype.scan = function (params, callback) {
  var self = this;

  return bindCallback(
    nodefn.call((this.client.scan).bind(this.client), params)
      .then(function (res) {

        if (res.Items && self.transform) {
          res.Items = res.Items.map(function (item) {
            return self.transform.from(item);
          });
        }

        return res;
      }),
    callback);
};

/**
 * returns a stream with query results
 *
 * @param {Object} params
 * @returns {exports.Query}
 */
DynamoDb.prototype.queryStream = function (params) {
  return new streams.Query(this, params);
};

/**
 * returns one or more items and its attributes by performing a full scan of a table.
 *
 * @param params
 * @returns {exports.Scan}
 */
DynamoDb.prototype.scanStream = function (params) {
  return new streams.Scan(this, params);
};

/**
 *
 * @param params
 * @returns {exports.BatchGet}
 */
DynamoDb.prototype.batchGetStream = function (params) {
  return new streams.BatchGet(this, params);
};

/**
 *
 * Returns a BatchWriteStream instance
 *
 * performs multiple put or delete operations
 * using the batchWriteItem method
 *
 * Caches up to 25 elements before calling the database
 *
 * @returns {exports.BatchWrite}
 */
DynamoDb.prototype.batchWriteStream = function () {
  return new streams.BatchWrite(this);
};

/**
 *
 * TODO add test
 *
 * Waits for a given DynamoDB resource. The final callback or 'complete' event will be fired only when the resource is either in its final state or the waiter has timed out and stopped polling for the final state.
 *
 * {@link http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#waitFor-property AWS Docs}.
 *
 * @param {String} state the resource state to wait for. Available states for this service are listed in "Waiter Resource States" below.
 * @param {Object=} params a list of parameters for the given state. See each waiter resource state for required parameters.
 * @param {Function=} callback
 * @returns {*}
 */
DynamoDb.waitFor = function (state, params, callback) {

  return bindCallback(
    nodefn.call((this.client.waitFor).bind(this.client), state, params),
    callback);
};

module.exports = DynamoDb;