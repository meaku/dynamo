"use strict";

var expect = require("chai").expect,
    localDb = require("../support/localDynamoDb");

var DynamoDB = require("../../lib"),
    dummies = require("../support/dummies/tables"),
    dummySchemas = require("../support/dummies/schemas");


describe("batch operations", function () {

    var db;

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

    beforeEach(function () {
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


    describe("#batchWriteItems", function () {

        it("should write a batch of item to the database", function () {

            var expectedItems = [],
                j;

            for (j = 1; j <= 10; j++) {
                expectedItems.push(generateItem(j));
            }

            db.setSchemas(dummySchemas);

            function generateItem(FileId) {
                return {

                    UserId: "mj",
                    FileId: "" + FileId + "",
                    Name: 'bla',
                    Size: 3,
                    ItemsOnMyDesk: ['a', 'b'],
                    testBoolean: true,
                    Pens: {a: 'aa', b: 'bb'},
                    Quantity: 12
                };
            }

            var batchRequest = {
                RequestItems: {
                    TestTable: []
                }
            };

            //populate
            for (j = 1; j <= 10; j++) {
                batchRequest.RequestItems.TestTable.push({
                    PutRequest: {
                        Item: generateItem(j)
                    }
                });
            }

            return db.batchWriteItem(batchRequest)
                .then(function (res) {

                    expect(res.UnprocessedItems).to.eql({});

                    batchRequest = {
                        RequestItems: {
                            TestTable: {
                                Keys: expectedItems.map(function(item) {
                                    return {
                                        UserId: item.UserId,
                                        FileId: item.FileId
                                    };
                                })
                            }
                        }
                    };

                    return db.batchGetItem(batchRequest);
                })
                .then(function (res) {

                    expect(res.UnprocessedKeys).to.eql({});

                    for(j = 0; j < 10; j++) {
                        expect(res.Responses.TestTable).to.contain(expectedItems[j]);

                    }
                });
        });
    });
});


