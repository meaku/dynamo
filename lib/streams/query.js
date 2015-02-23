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
  this.params = params;
  this.hasMore = true;

  this.items = [];

  Readable.call(this, {objectMode: true});
}

util.inherits(QueryStream, Readable);

QueryStream.prototype._read = function () {

  var self = this;

  //we're done!
  if (!this.hasMore && this.items.length === 0) {
    self.push(null);
    return;
  }

  if (this.exclusiveStartKey) {
    this.params.ExclusiveStartKey = this.exclusiveStartKey;
  }

  if (this.items.length !== 0) {
    self._doPush();
    return;
  }

  this._fetchFromDb();
};

QueryStream.prototype._doPush = function() {

  if(this.items.length > 0) {
    this.push(this.items.shift());
    return;
  }

  this.push(null);
};

QueryStream.prototype._fetchFromDb = function() {

  var self = this;

  this.db.query(this.params)
    .done(
    function (data) {

      self.hasMore = data && data.LastEvaluatedKey;

      //TODO check for unprocessed keys here!

      self.exclusiveStartKey = data.LastEvaluatedKey;
      self.items = data.Items;

      self._doPush();
    },
    function (err) {
      throw err;
    });

};

module.exports = QueryStream;