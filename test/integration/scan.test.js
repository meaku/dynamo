"use strict";

var expect = require("chai").expect,
    localDb = require("../support/localDynamoDb");

var DynamoDB = require("../../lib"),
    dummies = require("../support/dummies/tables"),
    dummyFactory = require("../support/dummyFactory");


describe("#scan", function () {

    var db,
        createdItems;

    function generateItem(idx) {
        return {
            UserId: "mj",
            FileId: "" + idx + "",
            Name: 'bla',
            Size: 3,
            ItemsOnMyDesk: ['a', 'b'],
            testBoolean: true,
            Pens: {a: 'aa', b: 'bb'},
            Quantity: 12
        };
    }

    this.timeout(50000);

    before(function () {

        db = new DynamoDB({
            apiVersion: "2014-04-24",
            endpoint: "http://localhost:8000"
        });

        return localDb.start();
    });

    beforeEach(function () {
        return db.deleteAllTables()
            .then(function () {
                return db.createTable(dummies.TestTable);
            })
            .then(function () {
                return dummyFactory.createItems(db, generateItem, 100);
            })
            .then(function (res) {
                createdItems = res;
            });
    });

    after(function () {
        return db.deleteAllTables()
            .then(function () {
                return localDb.stop();
            });
    });

    it("should return a table not found exception if table does not exist", function (done) {

        db.scan({
            TableName: "NonExisting",
            ScanFilter: {
                UserId: {
                    ComparisonOperator: "EQ",
                    AttributeValueList: [{S: "mj"}]
                }

            },
            Limit: 100
        })
            .catch(function (err) {
                expect(err).to.be.instanceOf(Error);
                done();
            });
    });

    it("should return the query response with transformed items", function () {

        return db.scan({
            TableName: dummies.TestTable.TableName,
            ScanFilter: {
                UserId: {
                    ComparisonOperator: "EQ",
                    AttributeValueList: [{S: "mj"}]
                }

            },
            Limit: 100
        })
            .then(function (res) {
                res.Items.forEach(function (item) {
                    expect(createdItems).to.contain(item);
                });
            });
    });
});


