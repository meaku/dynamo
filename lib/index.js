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

DynamoDb.prototype.listTables = function (params, callback) {
    return bindCallback(nodefn.call((this.client.listTables).bind(this.client), params), callback);
};

DynamoDb.prototype.describeTable = function (table, callback) {

    if (typeof table === "string") {
        table = {
            TableName: table
        };
    }

    return bindCallback(nodefn.call((this.client.describeTable).bind(this.client), table), callback);
};

DynamoDb.prototype.createTable = function (table, callback) {

    var self = this;

    if (typeof table === "string" && this.tables) {
        table = this.tables[table];
    }

    return bindCallback(when.promise(function (resolve, reject) {

        if (!table || typeof table !== "object") {
            reject(new Error("Table has to be an object"));
            return;
        }

        return nodefn.call((self.client.describeTable).bind(self.client), table)
    }), callback);
};

DynamoDb.prototype.updateTable = function (params, callback) {
    return bindCallback(nodefn.call((this.client.updateTable).bind(this.client), params), callback);
};

DynamoDb.prototype.deleteTable = function (table, callback) {

    if (typeof table === "string") {
        table = {
            TableName: table
        };
    }

    return bindCallback(nodefn.call((this.client.deleteTable).bind(this.client), table), callback);
};

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