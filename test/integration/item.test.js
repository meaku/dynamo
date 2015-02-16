"use strict";

var expect = require("chai").expect,
    localDb = require("../support/localDynamoDb"),
    when = require("when");

var DynamoDB = require("../../lib"),
    dummies = require("../support/dummies/tables"),
    dummySchemas = require("../support/dummies/schemas");

describe("Item", function () {
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
            })
            .then(function() {
                return db.hasTable(dummies.TestTable.TableName);
            })
            .then(function(hasTable) {
                if(!hasTable) {
                   throw new Error("Could not create Table '" + dummies.TestTable.TableName + "'");
                }
            });
    });

    after(function () {
        return db.deleteAllTables();
    });

    after(function () {
        return localDb.stop();
    });

    describe("#putItem", function () {

        it("should accept items in the fitting format and ignore transform if not set", function () {

            var data = {
                UserId: {S: "1"},
                FileId: {S: "xy"},
                Name: {S: "bla"},
                Size: {N: "3"},
                ItemsOnMyDesk: {
                    L: [
                        {S: "a"},
                        {S: "b"}
                    ]
                },
                Pens: {
                    M: {
                        a: {S: "aa"},
                        b: {S: "bb"}
                    }
                },
                testBoolean: {BOOL: true},
                Quantity: {N: "12"}

            };

            //disable transformation
            db.transform = null;

            return db.putItem({
                TableName: dummies.TestTable.TableName,
                Item: data
            })
                .then(function () {
                    return db.getItem({
                        TableName: dummies.TestTable.TableName,
                        Key: {
                            UserId: {S: "1"},
                            FileId: {S: "xy"}
                        }
                    });
                })
                .then(function (res) {
                    expect(res.Item).to.eql(data);
                });

        });

        it("should set an item and transform if schema has been defined", function () {

            db.setSchemas(dummySchemas);

            var data = {
                UserId: "1",
                FileId: "xy",
                Name: "bla",
                Size: 3,
                ItemsOnMyDesk: ["a", "b"],
                testBoolean: true,
                Pens: {a: "aa", b: "bb"},
                Quantity: 12
            };

            return db.putItem({
                TableName: dummies.TestTable.TableName,
                Item: data
            })
                .then(function () {
                    return db.getItem({
                        TableName: dummies.TestTable.TableName,
                        Key: {
                            UserId: 1,
                            FileId: "xy"
                        }
                    });
                })
                .then(function (res) {
                    expect(res.Item).to.eql(data);
                });
        });
    });

    describe("#getItem", function () {

        it("should return {} if an item could be found", function () {

            return db.getItem({
                TableName: dummies.TestTable.TableName,
                Key: {
                    UserId: {S: "2"},
                    FileId: {S: "xy"}
                }
            })
                .then(function (res) {
                    expect(res).to.eql({});
                });
        });

        it("should return an existing item", function () {

            var data = {
                UserId: {S: "1"},
                FileId: {S: "xy"},
                Name: {S: "bla"},
                Size: {N: "3"},
                ItemsOnMyDesk: {
                    L: [
                        {S: "a"},
                        {S: "b"}
                    ]
                },
                Pens: {
                    M: {
                        a: {S: "aa"},
                        b: {S: "bb"}
                    }
                },
                testBoolean: {BOOL: true},
                Quantity: {N: "12"}

            };

            //disable transformation
            db.transform = null;

            return db.putItem({
                TableName: dummies.TestTable.TableName,
                Item: data
            })
                .then(function () {
                    return db.getItem({
                        TableName: dummies.TestTable.TableName,
                        Key: {
                            UserId: {S: "1"},
                            FileId: {S: "xy"}
                        }
                    });
                })
                .then(function (res) {
                    expect(res.Item).to.eql(data);
                });
        });


        it("should transform request and response if schemas are set", function () {

            db.setSchemas(dummySchemas);

            var data = {
                UserId: "1",
                FileId: "xy",
                Name: "bla",
                Size: 3,
                ItemsOnMyDesk: ["a", "b"],
                testBoolean: true,
                Pens: {
                    a: "aa",
                    b: "bb"
                },
                Quantity: 12
            };

            return db.putItem({
                TableName: dummies.TestTable.TableName,
                Item: data
            })
                .then(function () {
                    return db.getItem({
                        TableName: dummies.TestTable.TableName,
                        Key: {
                            UserId: "1",
                            FileId: "xy"
                        }
                    });
                })
                .then(function (res) {
                    expect(res.Item).to.eql(data);
                });
        });
    });

    describe("#deleteItem", function () {

        function createDummy(data) {
            db.setSchemas(dummySchemas);

            return db.putItem({
                TableName: dummies.TestTable.TableName,
                Item: data
            })
                .then(function () {
                    return db.getItem({
                        TableName: dummies.TestTable.TableName,
                        Key: {
                            UserId: "1",
                            FileId: "xy"
                        }
                    });
                });
        }

        it("should delete an existing Item", function () {

            db.setSchemas(dummySchemas);

            var data = {
                UserId: "1",
                FileId: "xy",
                Name: "bla",
                Size: 3,
                ItemsOnMyDesk: ["a", "b"],
                testBoolean: true,
                Pens: {a: "aa", b: "bb"},
                Quantity: 12
            };

            createDummy(data)
                .then(function (res) {

                    delete db.transform;

                    expect(res.Item).to.eql(data);

                    return db.deleteItem({
                        TableName: dummies.TestTable.TableName,
                        Key: {
                            UserId: {S: "1"},
                            FileId: {S: "xy"}
                        }
                    });
                })
                .then(function () {
                    return db.getItem({
                        TableName: dummies.TestTable.TableName,
                        Key: {
                            UserId: {S: "1"},
                            FileId: {S: "xy"}
                        }
                    });
                })
                .then(function (res) {
                    expect(res).to.eql({});
                });
        });

        it("should delete an existing item transforming keys if schema exists", function () {

            db.setSchemas(dummySchemas);

            var data = {
                UserId: "1",
                FileId: "xy",
                Name: "bla",
                Size: 3,
                ItemsOnMyDesk: ["a", "b"],
                testBoolean: true,
                Pens: {a: "aa", b: "bb"},
                Quantity: 12
            };

            createDummy(data)
                .then(function (res) {

                    expect(res.Item).to.eql(data);

                    return db.deleteItem({
                        TableName: dummies.TestTable.TableName,
                        Key: {
                            UserId: "1",
                            FileId: "xy"
                        }
                    });
                })
                .then(function () {
                    return db.getItem({
                        TableName: dummies.TestTable.TableName,
                        Key: {
                            UserId: "1",
                            FileId: "xy"
                        }
                    });
                })
                .then(function (res) {
                    expect(res).to.eql({});
                });
        });
    });
});