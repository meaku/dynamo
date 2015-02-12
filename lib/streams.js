"use strict";

var util = require("util"),
    Readable = require("stream").Readable,
    when = require("when"),
    nodefn = require("when/node");

function QueryStream(client, params, transform) {

    this.client = client;
    this.params = params;
    this.transform = transform;

    this.items = [];

    Readable.call(this, { objectMode: true });
}

util.inherits(QueryStream, Readable);

QueryStream.prototype._read = function() {

    var self = this,
        item;

    function push() {
        item = self.items.shift();

        if(item && self.transform) {
            item = self.transform.from(self.params.TableName, item);
        }

        self.push(item);
    }

    if(this.exclusiveStartKey) {
        this.params.ExclusiveStartKey = this.exclusiveStartKey;
    }

    if(this.items.length !== 0) {
        push();
    }
    else {
        nodefn.call((this.client.query).bind(this.client), this.params)
            .done(function(data) {

                //might miss some data here
                if(data && !data.LastEvaluatedKey) {
                    self.push(null);
                    return;
                }

                self.exclusiveStartKey = data.LastEvaluatedKey;
                self.items = data.Items;

                push();
            }, function(err) {
                throw err;
            });
    }
};

function ScanStream(client, params, transform) {

    this.client = client;
    this.params = params;
    this.transform = transform;

    Readable.call(this, { objectMode: true });
}

util.inherits(ScanStream, Readable);

ScanStream.prototype._read = function() {

    var self = this,
        item;

    if(this.exclusiveStartKey) {
        this.params.ExclusiveStartKey = this.exclusiveStartKey;
    }

    function push() {
        item = self.items.shift();

        if(item && self.transform) {
            item = self.transform.from(self.params.TableName, item);
        }

        self.push(item);
    }

    if(this.items.length !== 0) {
        push();
    }
    else {

        nodefn.call((this.client.scan).bind(this.client), this.params)
            .done(function(data) {

                //might miss some data here
                if(data && !data.LastEvaluatedKey) {
                    self.push(null);
                    return;
                }

                self.exclusiveStartKey = data.LastEvaluatedKey;
                self.items = data.Items;

                push();
            }, function(err) {
                throw err;
            });
    }
};


exports.Query = QueryStream;
exports.Scan = ScanStream;
