"use strict";

var expect = require("chai").expect,
  localDb = require("../../support/localDynamoDb");

var DynamoDB = require("../../../lib"),
  dummies = require("../../support/dummies/tables");

describe("BatchWriteStream", function () {

  var db;

  function genDummyData(limit) {

    var items = [];

    for (var i = 1; i <= limit; i++) {
      items.push({
        UserId: "1",
        FileId: i.toString()
      });
    }

    return items;
  }

  this.timeout(50000);

  before(function () {
    return localDb.start();
  });

  before(function () {
    db = new DynamoDB({
      apiVersion: '2014-04-24',
      endpoint: "http://localhost:8000"
    });

    return db.deleteAllTables()
      .then(function () {
        return db.createTable(dummies.TestTable);
      });
  });

  after(function () {
    return db.deleteAllTables();
  });

  after(function () {
    return localDb.stop();
  });

  it("should return a writeable stream which signals backpressure", function (done) {

    db.useTransform();

    var dummyItems = genDummyData(50);
    var stream = db.batchWriteStream();

    dummyItems = dummyItems.map(function (item) {
      return {
        table: "TestTable",
        operation: "put",
        data: item
      };
    });

    stream.on("finish", done);

    stream.on("error", done);

    write();

    function write() {
      var ok = true,
        currentItem;

      while (ok && (currentItem = dummyItems.shift()) !== undefined) {

        if (dummyItems.length === 0) {
          stream.end(currentItem);
          break;
        }

        ok = stream.write(currentItem, function () {
        });

        if (!ok) {
          stream.once("drain", write);
          break;
        }
      }
    }
  });

  function expectItemsExist(expectedItems, done) {

    var queryStream = db.queryStream({
      TableName: dummies.TestTable.TableName,
      KeyConditions: {
        UserId: {
          ComparisonOperator: "EQ",
          AttributeValueList: [{S: "1"}]
        }

      },
      Limit: 20
    }),
      i = 0;

    queryStream.on("data", function (item) {
      i++;
      expect(expectedItems).to.contain(item);

    });

    queryStream.on("error", done);

    queryStream.on("end", function () {
      expect(i).to.eql(expectedItems.length);
      done();
    });

  }

  it("should handle end and flush accordingly", function (done) {

    var limit = 60,
      count = 0;

    db.useTransform();

    var baseDummyItems = genDummyData(limit);
    var stream = db.batchWriteStream();

    var dummyItems = baseDummyItems.map(function (item) {
      return {
        table: "TestTable",
        operation: "put",
        data: item
      };
    });

    stream.on("finish", function () {
      //end will be emitted as "finish" event
      expect(count).to.eql(limit - 1);

      expectItemsExist(baseDummyItems, done);
    });

    stream.on("error", done);

    function afterWrite(err) {
      count++;
      expect(err).to.eql(undefined);
    }

    write();

    function write() {
      var ok = true,
        currentItem;

      while (ok && (currentItem = dummyItems.shift()) !== undefined) {

        if (dummyItems.length === 0) {
          stream.end(currentItem);
          break;
        }

        ok = stream.write(currentItem, afterWrite);

        if (!ok) {
          stream.once("drain", write);
          break;
        }
      }
    }

  });
});