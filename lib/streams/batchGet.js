"use strict";

var util = require("util"),
    Readable = require("stream").Readable,
    nodefn = require("when/node");

function BatchGetStream(db, params) {

    this.db = db;
    this.params = params;
    this.items = [];

    Readable.call(this, {objectMode: true});
}

util.inherits(BatchGetStream, Readable);

BatchGetStream.prototype._read = function () {

    var self = this,
        requestItems = [],
        requestPayload;

    Object.keys(this.params.RequestItems).forEach(function (table) {
        self.params.RequestItems[table].Keys.forEach(function (key) {
            requestItems.push({table: table, key: key});
        });
    });

    function getNextBatch() {
        requestPayload = {};

        //fetch the next 25
        requestItems.splice(0, 25).forEach(function (requestItem) {

            //ensure table array
            requestPayload[requestItem.table] = requestPayload[requestItem.table] || { Keys: [] };

            //add keys
            requestPayload[requestItem.table].Keys.push(requestItem.key);
        });

        return requestPayload;
    }

    console.log("total requests", requestItems.length);

    if (this.exclusiveStartKey) {
        this.params.ExclusiveStartKey = this.exclusiveStartKey;
    }

    function push() {
        self.push(self.items.shift());
    }

    if (self.items.length !== 0) {
        push();
    }
    else {

        requestPayload = getNextBatch();

        console.log("left", requestItems.length, util.inspect(requestPayload, { depth: null}));

        //nothing to fetch
        if (requestItems.length === 0) {
            //done
            self.push(null);
            return;
        }

            self.db.batchGetItem({
                RequestItems: requestPayload
            })
                .done(
                function (res) {

                    console.log("done", res);

                    self.items = res.Items;

                    console.log(res.Item);

                    console.log("Unprocessed keys", Object.keys(res.UnprocessedKeys));

                    push();
                },
                function (err) {
                    throw err;
                });
    }
};

module.exports = BatchGetStream;

