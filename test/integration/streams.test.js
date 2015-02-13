"use strict";

var expect = require("chai").expect,
    localDb = require("../support/localDynamoDb");

var DynamoDB = require("../../lib"),
    dummies = require("../support/dummies/tables"),
    dummySchemas = require("../support/dummies/schemas");

describe("Streams", function () {
    var db;

    this.timeout(50000);

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
                i = 0,
                j,
                total = 25;


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
            for (j = 1; j <= total; j++) {
                expectedItems.push(generateItem(j));

                batchRequest.RequestItems.TestTable.push({
                    PutRequest: {
                        Item: generateItem(j)
                    }
                });
            }

            return db.batchWriteItem(batchRequest)
             .then(function (res) {

                    expect(res.UnprocessedItems).to.eql({});

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
});