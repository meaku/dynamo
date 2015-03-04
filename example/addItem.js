"use strict";

/**
 * Example of adding an item using promises
 *
 * @type {DynamoDb|exports}
 */

var DynamoDB = require("../lib");

var db = new DynamoDB({
    apiVersion: '2014-04-24',
    endpoint: "http://localhost:8000"
});

var setupDb = require("./support/setupDb");

db.setSchemas(require("../test/support/dummies/schemas"));

setupDb(db)
    .then(function () {

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

        //create!
        return db.putItem({
            TableName: "TestTable",
            Item: data
        })
            .then(function () {

                //fetch!
                return db.getItem({
                    TableName: "TestTable",
                    Key: {
                        UserId: {S: "1"},
                        FileId: {S: "xy"}
                    }
                });
            })

            .done(
            //called on success
            function (res) {
                console.log("created Item: ", res);
            },
            //called on error
            function (err) {
                console.log("An error occured: ", err.message);
            });
    });
