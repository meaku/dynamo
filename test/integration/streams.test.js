"use strict";

var expect = require("chai").expect,
    when = require("when"),
    localDb = require("../support/localDynamoDb");

var DynamoDB = require("../../lib"),
    dummies = require("../support/dummies/tables"),
    dummySchemas = require("../support/dummies/schemas");

describe("Streams", function () {
    var db;

    this.timeout(50000);

    function createDummyData(amount) {

        var createdItems = [];

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

        var j,
            batchRequest = {
                RequestItems: {
                    TestTable: []
                }
            },
            batchRequests = [];

        //populate
        for (j = 1; j <= amount; j++) {

            createdItems.push(generateItem(j));

            batchRequest.RequestItems.TestTable.push({
                PutRequest: {
                    Item: generateItem(j)
                }
            });
        }

        while (batchRequest.RequestItems.TestTable.length > 0) {
            batchRequests.push({
                RequestItems: {
                    TestTable: batchRequest.RequestItems.TestTable.splice(0, 25)
                }
            });
        }

        return when.map(batchRequests, function (batch) {
            return db.batchWriteItem(batch);
        })
            .then(function () {
                return createdItems;
            });
    }

    before(function () {
        return localDb.start();
    });

    before(function () {
        db = new DynamoDB({
            apiVersion: '2014-04-24',
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

    describe("#QueryStream", function () {

        it("should return a readable stream", function (done) {

            db.setSchemas(dummySchemas);

            var expectedItems = [],
                stream,
                total = 25,
                i = 0;

            createDummyData(total)
                .done(function (res) {

                    expectedItems = res;

                    stream = db.queryStream({
                        TableName: dummies.TestTable.TableName,
                        KeyConditions: {
                            UserId: {
                                ComparisonOperator: "EQ",
                                AttributeValueList: [{S: "mj"}]
                            }

                        },
                        Limit: 5
                    });

                    stream.on("data", function (item) {
                        i++;
                        expect(expectedItems).to.contain(item);
                    });

                    stream.on("error", function (err) {
                        throw err;
                    });

                    stream.on("end", function () {
                        expect(i).to.eql(total);
                        done();
                    });

                }, done);
        });
    });


    describe.only("#BatchReadStream", function () {

        it("should return a readable stream", function (done) {

            var stream,
                i = 0,
                expectedItems;

            db.setSchemas(dummySchemas);

            createDummyData(100)
                .done(function (res) {
                    expectedItems = res;

                    stream = db.batchGetStream({
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
                    });

                    stream.on("data", function (data) {
                        i++;
                        expect(expectedItems).to.contain(data.item);
                    });

                    stream.on("error", function (err) {
                        throw err;
                    });

                    stream.on("end", function () {
                        expect(i).to.eql(100);
                        done();
                    });

                }, done);


        });
    });
});