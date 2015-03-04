"use strict";

var expect = require("chai").expect,
    localDb = require("../support/localDynamoDb");

var DynamoDB = require("../../lib");

describe("Local DynamoDB", function () {

    describe("LocalDB Helper", function () {

        it("should be able to start and stop a local instance", function () {
            return localDb
                .start()
                .then(function (dbProcess) {
                    expect(dbProcess.pid).to.be.a("number");

                    return localDb.stop();
                });
        });
    });

    describe("Basics", function () {

        var db;

        before(function () {
            return localDb.start();
        });

        before(function() {
            db = new DynamoDB({
                apiVersion: '2014-04-24',
                endpoint: "http://localhost:8000"
            });
        });

        after(function () {
            return localDb.stop();
        });

        it("should be able to retrieve the tables the promise way", function () {
            return db.listTables({}).then(function(data) {
                expect(data.TableNames).to.eql([]);
            });
        });

        it("should be able to retrieve the tables the async way", function (done) {
            db.listTables({}, function(err, data) {
                if(err) {
                    done(err);
                    return;
                }

                expect(data.TableNames).to.eql([]);
                done();
            });
        });
    });
});




