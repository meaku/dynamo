"use strict";

var localDb = require("../test/support/localDynamoDb");

localDb
    .start()
    .done(function () {
        console.log("Local DynamoDB instance up & running");
    },
    function (err) {
        throw err;
    });