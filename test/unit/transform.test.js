"use strict";

var expect = require("chai").expect,
    inspect = require("util").inspect;

var transform = require("../../lib/transform"),
    schemas = require("../support/dummies/schemas");

describe("transform", function () {

    var t;

    before(function() {
        t = transform(schemas);
    });

    describe("to", function () {

        it("should throw if item is missing", function() {

           expect(function() {
               t.to("TestTable");
           }).to.throwError;

        });

        it("should convert an object to the dynamodb represenation", function() {

            var obj = {
                UserId: 1,
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

            var res = t.to("TestTable", obj);

            //console.log(inspect(res, {depth: 100}));

            expect(res).to.eql({
                UserId: {S: '1'},
                FileId: {S: 'xy'},
                Name: {S: 'bla'},
                Size: {N: '3'},
                ItemsOnMyDesk: {
                    L: [
                        {S: 'a'},
                        {S: 'b'}
                    ]
                },
                Pens: {
                    M: {
                        a: {S: 'aa'},
                        b: {S: 'bb'}
                    }
                },
                testBoolean: {BOOL: true},
                Quantity: {N: '12'}
            });

        });

    });


     describe("from", function () {

         it("should throw if item is missing", function() {

             expect(function() {
                 t.from("TestTable");
             }).to.throwError;

         });

         it("should convert a dynamodb represenation back to an object", function() {

             var dynamoData = {
                 UserId: {S: '1'},
                 FileId: {S: 'xy'},
                 Name: {S: 'bla'},
                 Size: {N: '3'},
                 ItemsOnMyDesk: {
                     L: [
                         {S: 'a'},
                         {S: 'b'}
                     ]
                 },
                 Pens: {
                     M: {
                         a: {S: 'aa'},
                         b: {S: 'bb'}
                     }
                 },
                 testBoolean: {BOOL: true},
                 Quantity: {N: '12'}
             };

             var res = t.from("TestTable", dynamoData);

             //console.log(inspect(res, {depth: 100}));

             expect(res).to.eql({
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
             });
         });
    });
});