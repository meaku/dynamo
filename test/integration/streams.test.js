"use strict";

var expect = require("chai").expect,
    localDb = require("../support/localDynamoDb");

var DynamoDB = require("../../lib"),
    dummies = require("../support/dummies/tables"),
    dummyFactory = require("../support/dummyFactory"),
    dummySchemas = require("../support/dummies/schemas");

describe("Streams", function () {
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

    function genDummyData(limit) {

        var items = [];

        for (var i = 1; i <= limit; i++) {
            items.push({
                UserId: "1",
                FileId: i
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
    });

    after(function () {
        return db.deleteAllTables();
    });

    after(function () {
        return localDb.stop();
    });

    describe("#QueryStream", function () {

        before(function () {
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

        it("should return a readable stream", function (done) {

            db.setSchemas(dummySchemas);

            var stream,
                total = 100,
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

    describe("#ScanStream", function () {

        before(function () {
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

        it("should return a readable stream", function (done) {

            db.setSchemas(dummySchemas);

            var stream,
                total = 100,
                i = 0;

            stream = db.scanStream({
                TableName: dummies.TestTable.TableName,
                ScanFilter: {
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

    describe("#BatchReadStream", function () {

        before(function () {
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

            db.setSchemas(dummySchemas);

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

    describe("#BatchWriteStream", function () {

        before(function () {
            return db.deleteAllTables()
                .then(function () {
                    return db.createTable(dummies.TestTable);
                });
        });

        it("should return a writeable stream which signals backpressure", function (done) {

            db.setSchemas(dummySchemas);

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
                        stream.end(currentItem, function (err) {
                        });
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

        it("should handle end and flush accordingly", function (done) {

            var limit = 60,
                count = 0;

            db.setSchemas(dummySchemas);

            var dummyItems = genDummyData(limit);
            var stream = db.batchWriteStream();

            dummyItems = dummyItems.map(function (item) {
                return {
                    table: "TestTable",
                    operation: "put",
                    data: item
                };
            });

            stream.on("finish", function() {
                //end will be emitted as "finish" event
                expect(count).to.eql(limit-1);
                done();
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
});