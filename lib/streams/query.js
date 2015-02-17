"use strict";

var util = require("util"),
    Readable = require("stream").Readable;

/**
 *
 * QueryStream
 *
 * @param db {Object}
 * @param params {Object}
 * @constructor
 */
function QueryStream(db, params) {

    this.db = db;
    this.transform = db.transform;
    this.params = params;

    this.items = [];

    Readable.call(this, {objectMode: true});
}

util.inherits(QueryStream, Readable);

QueryStream.prototype._read = function () {

    var self = this;

    function push() {
        self.push(self.items.shift());
    }

    if (this.exclusiveStartKey) {
        this.params.ExclusiveStartKey = this.exclusiveStartKey;
    }

    if (this.items.length !== 0) {
        push();
    }
    else {
        self.db.query(self.params)
            .done(
            function (data) {
                if(data && !data.LastEvaluatedKey) {
                    self.push(null);
                    return;
                }

                //TODO check for unprocessed keys here!

                self.exclusiveStartKey = data.LastEvaluatedKey;
                self.items = data.Items;

                push();
            },
            function (err) {
                throw err;
            });
    }
};

module.exports = QueryStream;