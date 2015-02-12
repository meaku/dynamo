"use strict";

/**
 * Example of creating a dynamo table using an async style
 *
 * @type {DynamoDb|exports}
 */

var DynamoDB = require("../lib");

var db = new DynamoDB({
    apiVersion: '2014-04-24',
    endpoint: "http://localhost:8000"
});

var tableDefinition = {
    TableName: 'TestTable', // required
    AttributeDefinitions: [
        { AttributeName: "UserId", AttributeType: "S" },
        { AttributeName: "FileId", AttributeType: "S" },
        { AttributeName: "Type", AttributeType: "S" }
    ],
    KeySchema: [ // required
        { AttributeName: 'UserId', KeyType: 'HASH'},
        { AttributeName: 'FileId', KeyType: 'RANGE'}
    ],
    ProvisionedThroughput: { // required
        ReadCapacityUnits: 10, // required
        WriteCapacityUnits: 10 // required
    },
    GlobalSecondaryIndexes: [
        {
            IndexName: 'TypeIndex', // required
            KeySchema: [ // required
                { AttributeName: 'UserId', KeyType: 'HASH'},
                { AttributeName: 'Type', KeyType: 'RANGE'}
            ],
            Projection: { // required
                ProjectionType: 'ALL'
            },
            ProvisionedThroughput: { // required
                ReadCapacityUnits: 1, // required
                WriteCapacityUnits: 1 // required
            }
        }
    ]
};

db.createTable(tableDefinition, function(err, res) {

    if(err) {
        throw err;
    }

    console.log("table created", res);
});

