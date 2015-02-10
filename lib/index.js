"use strict";

var when = require("when"),
    nodefn = require("when/node"),
    poll = require("when/poll"),
    bindCallback = nodefn.bindCallback,
    AWSDynamoDB = require("aws-sdk").DynamoDB,
    transform = require("./transform");


function DynamoDb() {
    DynamoDb.prototype.constructor.apply(this, arguments);
}

DynamoDb.prototype.constructor = function (options) {
    this.client = new AWSDynamoDB(options);
};

DynamoDb.prototype.setTables = function (tables) {
    this.tables = tables;
};

DynamoDb.prototype.setSchemas = function (schemas) {
    this.schemas = schemas;
    this.transform = transform(schemas);
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

    if (typeof table === "string" && this.tables) {
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
 * @TODO same as describe table, but tries multiple times?
 * @TODO maybe return true/false, at least that's what i'd expect
 * @TODO Add a max poll check to prevent endless polling
 *
 * @param {String|Object} table
 * @param {Function=} callback
 * @returns {Promise}
 */
DynamoDb.prototype.hasTable = function (table, callback) {
    var self = this;

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
            2000,
            function condition(data) {
                return data && data.Table.TableStatus === "ACTIVE";
            }),
        callback);
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
        //TODO no sure if we should solve it this way?
        params.Item = this.transform.to(params.TableName, params.Item);
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
 *
 * @param params
 * @param callback
 * @returns {Promise}
 */
DynamoDb.prototype.getItem = function (params, callback) {

    var self = this;

    return bindCallback(
        nodefn.call((this.client.getItem).bind(this.client), params)
            .then(function (data) {

                if (self.transform) {
                    //TODO no sure if we should solve it this way?
                    return self.transform.from(params.TableName, data);
                }

                return data;
            }),
        callback
    );
};


module.exports = DynamoDb;