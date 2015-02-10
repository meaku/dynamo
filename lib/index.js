"use strict";

var when = require("when"),
    nodefn = require("when/node"),
    poll = require("when/poll"),
    bindCallback = nodefn.bindCallback,
    AWSDynamoDB = require("aws-sdk").DynamoDB;


function DynamoDb() {
    DynamoDb.prototype.constructor.apply(this, arguments);
}

DynamoDb.prototype.constructor = function (options) {
    this.client = new AWSDynamoDB(options);
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


module.exports = DynamoDb;