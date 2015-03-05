"use strict";

var expect = require("chai").expect;

var transform = require("../../lib/transform");

describe("transform", function () {

  describe("to", function () {

    it("should throw if item is missing", function () {

      expect(function () {
        transform.to();
      }).to.throwError;

    });

    it("should convert an object to the dynamodb represenation", function () {

      var obj = {
        UserId: 1,
        FileId: "xy",
        Name: "bla",
        Size: 3,
        ItemsOnMyDesk: ["a", 1],
        testBoolean: true,
        Pens: {
          a: "aa",
          b: "bb"
        },
        Quantity: 12
      };

      var res = transform.to(obj);

      expect(res).to.eql({
        UserId: {N: "1"},
        FileId: {S: "xy"},
        Name: {S: "bla"},
        Size: {N: "3"},
        ItemsOnMyDesk: {
          L: [
            {S: "a"},
            {N: "1"}
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
      });

    });

  });


  describe("from", function () {

    it("should throw if item is missing", function () {

      expect(function () {
        transform.from();
      }).to.throwError;

    });

    it("should convert a dynamodb represenation back to an object", function () {

      var dynamoData = {
        UserId: {N: "1"},
        FileId: {S: "xy"},
        Name: {S: "bla"},
        Size: {N: "3"},
        ItemsOnMyDesk: {
          L: [
            {S: "a"},
            {N: 1}
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

      var res = transform.from(dynamoData);

      expect(res).to.eql({
        UserId: 1,
        FileId: "xy",
        Name: "bla",
        Size: 3,
        ItemsOnMyDesk: ["a", 1],
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