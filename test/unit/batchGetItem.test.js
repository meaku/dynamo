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

describe("batchGetItem", function () {

  it("should not retry if unprocessedKeys is an empty object", function () {

    var responses = {
      TableA: [
        {id: 1},
        {id: 2}
      ],
      TableB: [
        {id: 3},
        {id: 4}
      ]
    };

    db.client.batchGetItem = function mockedBatchItem(params, callback) {

      setImmediate(function () {
        callback(null, {
          Responses: responses,
          UnprocessedKeys: {}
        });
      });
    };

    return db.batchGetItem({
      RequestItems: []
    })
      .then(function (res) {
        expect(res.Responses).to.eql(responses);
      });

  });

  it("should retry until unprocessedKeys contains no more keys", function () {

    var responses = {
        TableA: [
          {id: 1},
          {id: 2}
        ]
      },
      callCount = 0;

    db.client.batchGetItem = function mockedBatchItem(params, callback) {

      responses = {
        0: {
          Responses: {
            TableA: [
              {id: 1},
              {id: 2}
            ]
          },
          UnprocessedKeys: {
            TableB: {
              Keys: [
                {id: 3},
                {id: 4}
              ]
            }
          }
        },
        1: {
          Responses: {
            TableB: [
              {id: 3},
              {id: 4}
            ]
          },
          UnprocessedKeys: {}
        }
      };

      setImmediate(callback, null, responses[callCount++]);
    };

    return db.batchGetItem({
      RequestItems: {
        TableA: {
          Keys: [
            {id: 1},
            {id: 2}
          ]
        },
        TableB: {
          Keys: [
            {id: 3},
            {id: 4}
          ]
        }
      }
    })
      .then(function (res) {

        expect(res.Responses).to.eql({
          TableA: [
            {id: 1},
            {id: 2}
          ],
          TableB: [
            {id: 3},
            {id: 4}
          ]
        });
      });

  });

  it("should try max 5 times", function () {

    var callCount = 0;

    db.client.batchGetItem = function mockedBatchItem(params, callback) {
      callCount++;

      setImmediate(callback, null, {
        Responses: {},
        UnprocessedKeys: {
          TableB: {
            Keys: [
              {id: 3},
              {id: 4}
            ]
          }
        }
      });
    };

    return db.batchGetItem({})
      .then(function () {
        throw new Error("Should not be called");
      })
      .catch(function(err) {
        expect(callCount).to.eql(5);
        expect(err.message).to.eql("Retry limit exceeded");
      });
  });

});