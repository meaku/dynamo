"use strict";

var util = require("util"),
    Readable = require("stream").Readable;

function BatchGetStream(db, params) {

    var self = this;

    this.db = db;
    this.params = params;
    this.items = [];
    this.requestItems = [];

    //flatten structure
    Object.keys(this.params.RequestItems).forEach(function (table) {
        self.params.RequestItems[table].Keys.forEach(function (key) {
            self.requestItems.push({
                table: table,
                key: key
            });
        });
    });

    Readable.call(this, {objectMode: true});
}

util.inherits(BatchGetStream, Readable);

BatchGetStream.prototype._read = function () {

    var self = this,
        requestPayload;

    function getNextBatch() {
        requestPayload = {};

        //fetch the next 25
        self.requestItems.splice(0, 25).forEach(function (requestItem) {

            //ensure table array
            requestPayload[requestItem.table] = requestPayload[requestItem.table] || {Keys: []};

            //add keys
            requestPayload[requestItem.table].Keys.push(requestItem.key);
        });

        return requestPayload;
    }


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

        //nothing left to fetch
        if (self.requestItems.length === 0) {
            //done
            self.push(null);
            return;
        }

        requestPayload = getNextBatch();

        self.db.batchGetItem({
            RequestItems: requestPayload
        })
            .done(
            function (res) {

                self.items = [];

                /**
                 *  converted nested to flat structure
                 *
                 * {
                 *   TestTable: [Item1, Item2],
                 *   BlubTable: [Item1, Item2]
                 * }
                 *
                 *  ->
                 *
                 *  {
                 *    table: "TestTable", item: Item1
                 *  },
                 *  {
                 *    table: "TestTable", item: Item2
                 *  }
                 *
                 *
                 */
                Object.keys(res.Responses).forEach(function (tableName) {

                    res.Responses[tableName].forEach(function(item) {
                        self.items.push({
                            table: tableName,
                            item: item
                        });
                    });
                });

                push();
            },
            function (err) {
                throw err;
            });
    }
};

module.exports = BatchGetStream;

