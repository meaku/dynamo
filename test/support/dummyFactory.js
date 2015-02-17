"use strict";

var when = require("when"),
    dummySchemas = require("./dummies/schemas");

function createItems(db, generateItem, amount) {

    var createdItems = [];

    db.setSchemas(dummySchemas);

    /*
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
    */

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

exports.createItems = createItems;