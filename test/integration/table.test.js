"use strict";

var expect = require("chai").expect,
    localDb = require("../support/localDynamoDb");

var DynamoDB = require("../../lib"),
    dummies = require("../support/dummies/tables");

function expectValidTableDescription(table, tableName) {
    expect(table).to.be.an("object");
    expect(table).to.contain.keys(["AttributeDefinitions", "CreationDateTime", "TableSizeBytes"]);
    expect(table.TableName).to.eql(tableName);
}

function expectTableNonExistingError(err) {
    expect(err).to.be.instanceof(Error);
    expect(err.code).to.eql("ResourceNotFoundException");
}

describe("Table", function () {
    var db;

    this.timeout(5000);

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
        return db.deleteAllTables();
    });

    after(function () {
        return localDb.stop();
    });

    describe("#listTables", function () {

        describe("with empty database", function () {

            it("should return an empty array", function () {

                return db.listTables({}).then(function (data) {
                    expect(data.TableNames).to.eql([]);
                });
            });
        });

        describe("with existing tables", function () {

            it("should return an array containing the table names", function () {

                return db.createTable(dummies.TestTable)
                    .then(function () {
                        return db.listTables();
                    })
                    .then(function (data) {
                        expect(data.TableNames).to.eql([dummies.TestTable.TableName]);
                    });
            });
        });
    });

    describe("#deleteTable", function () {

        describe("with a missing table", function () {

            it("should return a 'ResourceNotFoundException'", function () {

                return db.deleteTable("NonExisting")
                    .catch(function (err) {
                        expectTableNonExistingError(err);
                    });
            });
        });

        describe("with an existing table", function () {

            it("should delete the table and return table data", function () {

                return db.createTable(dummies.TestTable)
                    .then(function () {
                        return db.deleteTable(dummies.TestTable.TableName);
                    })
                    .then(function (res) {
                        expect(res).to.be.an("object");
                        expect(res.TableDescription.TableName).to.eql(dummies.TestTable.TableName);
                    });
            });
        });
    });

    describe("#createTable", function () {

        it("should create a table if passed a valid object", function () {

            return db.createTable(dummies.TestTable)
                .then(function (res) {
                    expectValidTableDescription(res.TableDescription, dummies.TestTable.TableName);
                });
        });

        //TODO add test for create via string!
    });

    describe("#describeTable", function () {

        it("should fail if table does not exist", function () {

            return db.describeTable(dummies.TestTable.TableName)
                .catch(function (err) {
                    expectTableNonExistingError(err);
                });
        });

        it("should return a table description for an existing table if passed a valid string", function () {

            return db.createTable(dummies.TestTable)
                .then(function () {
                    return db.describeTable(dummies.TestTable.TableName);
                })
                .then(function (res) {
                    expectValidTableDescription(res.Table, dummies.TestTable.TableName);
                });
        });

        it("should return a table description for an existing table if passed a valid object", function () {

            return db.createTable(dummies.TestTable)
                .then(function () {
                    return db.describeTable({TableName: dummies.TestTable.TableName});
                })
                .then(function (res) {
                    expectValidTableDescription(res.Table, dummies.TestTable.TableName);
                });
        });
    });

    describe("#hasTable", function () {

        it("should fail if table does not exist", function () {

            return db.hasTable(dummies.TestTable.TableName)
                .catch(function (err) {
                    expectTableNonExistingError(err);
                });
        });

        it("should return the table description if a table exists", function () {
            return db.createTable(dummies.TestTable)
                .then(function () {
                    return db.describeTable(dummies.TestTable.TableName);
                })
                .then(function (res) {
                    expectValidTableDescription(res.Table, dummies.TestTable.TableName);
                });
        });
    });

    describe("#updateTable", function () {

        it("should fail if table name wasn't set in params", function () {
            return db.updateTable({})
                .catch(function (err) {
                    expect(err).to.be.instanceof(Error);
                    expect(err.code).to.eql("MissingRequiredParameter");
                });
        });

        it("should fail if table does not exist", function () {
            return db.updateTable({
                TableName: dummies.TestTable.TableName
            })
                .catch(function (err) {
                    expectTableNonExistingError(err);
                });
        });

        it("should update throughput if table exists", function () {
            return db.createTable(dummies.TestTable)
                .then(function () {
                    return db.updateTable({
                        TableName: dummies.TestTable.TableName,
                        ProvisionedThroughput:  {
                            ReadCapacityUnits: 50,
                            WriteCapacityUnits: 60
                        }
                    });
                })
                .then(function (res) {
                    expectValidTableDescription(res.TableDescription, dummies.TestTable.TableName);
                    expect(res.TableDescription.ProvisionedThroughput.ReadCapacityUnits).to.eql(50);
                    expect(res.TableDescription.ProvisionedThroughput.WriteCapacityUnits).to.eql(60);
                });
        });
    });
});