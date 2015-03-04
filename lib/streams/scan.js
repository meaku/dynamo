"use strict";

var util = require("util"),
    Readable = require("stream").Readable;

function ScanStream(db, params) {

    this.db = db;
    this.params = params;
    this.items = [];

    Readable.call(this, { objectMode: true });
}

util.inherits(ScanStream, Readable);

ScanStream.prototype._read = function() {

    var self = this;

    if(this.exclusiveStartKey) {
        this.params.ExclusiveStartKey = this.exclusiveStartKey;
    }

    function push() {
        self.push(self.items.shift());
    }

    if(this.items.length !== 0) {
        push();
    }
    else {
        self.db.scan(self.params)
            .done(function(data) {

                //TODO might miss some data here
                if(data && !data.LastEvaluatedKey) {
                    self.push(null);
                    return;
                }

                //TODO check for unprocessed keys here!

                self.exclusiveStartKey = data.LastEvaluatedKey;
                self.items = data.Items;

                push();
            }, function(err) {
                throw err;
            });
    }
};

module.exports = ScanStream;

