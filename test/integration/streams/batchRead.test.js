"use strict";

var expect = require("chai").expect,
    localDb = require("../../support/localDynamoDb");

var DynamoDB = require("../../../lib"),
    dummies = require("../../support/dummies/tables"),
    dummyFactory = require("../../support/dummyFactory");

describe("BatchReadStream", function () {
    var db,
        dummyItems;

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

    afterEach(function () {
        return db.deleteAllTables();
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
                dummyItems = res;
            });
    });

    it("should return a readable stream emitting table and item properties", function (done) {

        var stream,
            i = 0;

        db.useTransform();

        stream = db.batchGetStream({
            RequestItems: {
                TestTable: {
                    Keys: dummyItems.map(function (item) {
                        return {
                            UserId: item.UserId,
                            FileId: item.FileId
                        };
                    })
                }
            }
        });

        stream.on("data", function (data) {
            i++;
            expect(data).to.have.keys("item", "table");
            expect(data.table).to.eql("TestTable");

            expect(dummyItems).to.contain(data.item);
        });

        stream.on("error", function (err) {
            throw err;
        });

        stream.on("end", function () {
            expect(i).to.eql(100);
            done();
        });
    });
});