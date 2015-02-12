"use strict";

var expect = require("chai").expect,
    localDb = require("../support/localDynamoDb"),
    when = require("when");

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

            var baseData = {
                UserId: "mj",
                FileId: "0",
                Name: 'bla',
                Size: 3,
                ItemsOnMyDesk: ['a', 'b'],
                testBoolean: true,
                Pens: {a: 'aa', b: 'bb'},
                Quantity: 12
            };

            when.iterate(
                function (x) {
                    return x + 1;
                },

                function (i) {
                    return i >= 500;
                },
                function (i) {

                    baseData.FileId = i;

                    return db.putItem({
                        TableName: dummies.TestTable.TableName,
                        Item: baseData
                    });

                },
                0
            )
                .done(function (data) {

                    console.log("then");

                    var stream = db.queryStream({
                        TableName: dummies.TestTable.TableName,
                        KeyConditions: {
                            UserId: {
                                ComparisonOperator: "EQ",
                                AttributeValueList: [{S: "mj"}]
                            }

                        },
                        Limit: 100
                    });

                    /*
                     stream.on("readable", function() {
                     var chunk;
                     while (null !== (chunk = stream.read())) {
                     console.log('got %d bytes of data', chunk.length);
                     }

                     });
                     //*/

                    ///*
                    stream.on("data", function (chunk) {
                        i++;
                        console.log("data", i, chunk);
                        //console.log("data", chunk.Items.length, chunk.LastEvaluatedKey.FileId.S);
                    });
                    //*/

                    //stream.resume();

                    var i = 0;

                    stream.on("error", function (err) {
                        console.log("err", err);
                        throw err;
                    });

                    stream.on("end", function (err) {
                        console.log("end", i);
                    });

                    /*
                     stream.on("readable", function() {
                     console.log("readable", stream.read());
                     });
                     */

                }, done);


        });
    });
});