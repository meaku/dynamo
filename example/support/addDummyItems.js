"use strict";

var when = require("when"),
    dummySchemas = require("../../test/support/dummies/schemas"),
    setupDb = require("./setupDb");

function addDummyItems(db) {

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

    return setupDb(db)
        .then(function () {

            //TODO replace with batch create!
            return when.iterate(
                function (x) {
                    return x + 1;
                },

                function (i) {
                    return i >= 500;
                },
                function (i) {

                    baseData.FileId = i;

                    return db.putItem({
                        TableName: "TestTable",
                        Item: baseData
                    });

                },
                0
            );
        });
}

module.exports = addDummyItems;