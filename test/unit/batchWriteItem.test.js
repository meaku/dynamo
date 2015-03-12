"use strict";

var expect = require("chai").expect,
  DynamoDB = require("../../lib"),
  db;

beforeEach(function () {
  db = new DynamoDB({
    apiVersion: "2014-04-24",
    endpoint: "http://localhost:8000"
  });

  db.useTransform(false);
});

describe("batchWriteItem", function () {

  it("should not retry if unprocessedItems is an empty object", function () {

    var callCount = 0;

    db.client.batchWriteItem = function mockedWriteItem(params, callback) {

      callCount++;

      setImmediate(function () {
        callback(null, {
          UnprocessedItems: {}
        });
      });
    };

    return db.batchWriteItem({
      RequestItems: []
    })
      .then(function () {
        expect(callCount).to.eql(1);
      });

  });

  it("should retry until unprocessedItems contains no more items", function () {

    var callCount = 0,
      responses = {
        1: {
          UnprocessedItems: {
            TableA: {
              PutRequest: {

              }
            }
          }
        },
        2: {
          UnprocessedItems: {}
        }
      };

    db.client.batchWriteItem = function mockedWriteItem(params, callback) {

      callCount++;

      setImmediate(function () {
        callback(null, responses[callCount]);
      });
    };

    return db.batchWriteItem({
      RequestItems: []
    })
      .then(function () {
        expect(callCount).to.eql(2);
      });

  });

  it("should try max 5 times", function () {

    var callCount = 0;

    db.client.batchWriteItem = function batchWriteItem(params, callback) {
      callCount++;

      setImmediate(callback, null, {
        Responses: {},
        UnprocessedItems: {
          TableA: {
            PutRequest: {

            }
          }
        }
      });
    };

    return db.batchWriteItem({})
      .then(function () {
        throw new Error("Should not be called");
      })
      .catch(function(err) {
        expect(callCount).to.eql(5);
        expect(err.message).to.eql("Retry limit exceeded");
      });
  });

});