"use strict";

var expect = require("chai").expect,
  localDb = require("../../support/localDynamoDb");

var DynamoDB = require("../../../lib"),
  dummies = require("../../support/dummies/tables"),
  dummyFactory = require("../../support/dummyFactory"),
  dummySchemas = require("../../support/dummies/schemas");

describe("QueryStream", function () {
  var db,
    dummyItems,
    total = 100;

  function generateItem(idx) {
    return {
      UserId: "mj",
      FileId: "" + idx + "",
      Name: "bla",
      Size: 3,
      ItemsOnMyDesk: ["a", "b"],
      testBoolean: true,
      Pens: {a: "aa", b: "bb"},
      Quantity: 12
    };
  }

  this.timeout(50000);

  before(function () {
    return localDb.start();
  });

  before(function () {
    db = new DynamoDB({
      apiVersion: "2014-04-24",
      endpoint: "http://localhost:8000"
    });
  });

  after(function () {
    return localDb.stop();
  });

  beforeEach(function () {
    return db.deleteAllTables()
      .then(function () {
        return db.createTable(dummies.TestTable);
      })
      .then(function () {
        return dummyFactory.createItems(db, generateItem, total);
      })
      .then(function (res) {
        dummyItems = res;
      });
  });

  afterEach(function () {
    return db.deleteAllTables();
  });

  it("should return a readable stream", function (done) {

    db.setSchemas(dummySchemas);

    var stream,
      i = 0;

    stream = db.queryStream({
      TableName: dummies.TestTable.TableName,
      KeyConditions: {
        UserId: {
          ComparisonOperator: "EQ",
          AttributeValueList: [{S: "mj"}]
        }

      },
      Limit: 25
    });

    stream.on("data", function (item) {
      i++;
      expect(dummyItems).to.contain(item);
    });

    stream.on("error", function (err) {
      throw err;
    });

    stream.on("end", function () {
      expect(i).to.eql(total);
      done();
    });
  });
});