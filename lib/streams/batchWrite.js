"use strict";

var util = require("util"),
    Writable = require("stream").Writable;

/**
 * BatchWrite Stream
 *
 * performs multiple put or delete operations
 * using the batchWriteItem method
 *
 * Caches up to 25 elements before calling the database
 *
 * @param db
 * @constructor
 */
function BatchWriteStream(db) {

    this.db = db;
    this.chunks = [];

    Writable.call(this, {objectMode: true, highWaterMark: 25});
}

util.inherits(BatchWriteStream, Writable);

/**
 * cache chunk and write to db as soon as we reached 25 chunks
 * the callback does not necessarily mean that the item was written
 * but rather that it has been cached
 *
 * @param chunk
 * @param encoding
 * @param callback
 * @private
 */
BatchWriteStream.prototype._write = function (chunk, encoding, callback) {

    var self = this;
    this.chunks.push(chunk);

    if (this.chunks.length < 25 && chunk !== this.lastChunk) {
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

var superEnd = BatchWriteStream.prototype.end;

/**
 * overwrite original end to mark the latest chunk
 * when write is called with the latest chunk, we
 * flush no matter how many chunks are currently buffered
 *
 * @param chunk
 * @param encoding
 * @param cb
 */
BatchWriteStream.prototype.end = function(chunk, encoding, cb) {

    //mark last chunk to trigger flushing
    this.lastChunk = chunk;
    superEnd.apply(this, arguments);
};

/**
 * var chunk = {
 *   table: "TestTable",
 *   operation: "put",
 *   data: {}
 * }
 *
 * write all given chunks to the database
 *
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

    return this.db.batchWriteItem({
        RequestItems: payload
    });
};

module.exports = BatchWriteStream;