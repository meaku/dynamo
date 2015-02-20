"use strict";

var util = require("util"),
    Writable = require("stream").Writable;

function BatchWriteStream(db) {

    this.db = db;
    this.chunks = [];

    Writable.call(this, {objectMode: true, highWaterMark: 25});
}

util.inherits(BatchWriteStream, Writable);


BatchWriteStream.prototype._write = function (chunk, encoding, callback) {

    var self = this;
    this.chunks.push(chunk);

    if (this.chunks.length < 25) {
        setImmediate(callback);
        return;
    }

    this._writeToDb(this.chunks)
        .done(
        function onSuccess() {
            self.chunks = [];
            callback();
        },
        function onError(err) {
            callback(err);
        });
};

/**
 * var chunk = {
 *   table: "TestTable",
 *   operation: "put",
 *   data: {}
 * }
 * @param chunks
 */
BatchWriteStream.prototype._writeToDb = function (chunks) {

    var payload = {};

    chunks.forEach(function (chunk) {

        if (!payload[chunk.table]) {
            payload[chunk.table] = [];
        }

        if (chunk.operation === "put") {
            payload[chunk.table].push({
                PutRequest: {
                    Item: chunk.data
                }
            });
        }

        if (chunk.operation === "delete") {
            payload[chunk.table].push({
                DeleteRequest: {
                    Item: chunk.data
                }
            });
        }
    });

    //console.log(util.inspect(payload, {depth: null}));

    return this.db.batchWriteItem({
        RequestItems: payload
    });
};

module.exports = BatchWriteStream;