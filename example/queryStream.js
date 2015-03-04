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

var addDummyItems = require("./support/addDummyItems.js");

db.setSchemas(require("../test/support/dummies/schemas"));

addDummyItems(db)
    .then(function () {


        var stream = db.queryStream({
            TableName: "TestTable",
            KeyConditions: {
                UserId: {
                    ComparisonOperator: "EQ",
                    AttributeValueList: [{S: "mj"}]
                }

            },
            Limit: 100
        });

        ///*
        stream.on("readable", function () {
            var chunk;

            while (null !== (chunk = stream.read())) {
                console.log(chunk);
            }
        });
        //*/

        /*
         stream.on("data", function (chunk) {
         console.log("data", chunk);
         });
         //*/

        //stream.pipe(process.stdout);

        stream.on("error", function (err) {
            throw err;
        });


    });
