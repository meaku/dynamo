"use strict";

/**
 * @type {QueryStream|exports}
 */
exports.Query = require("./query.js");

/**
 *
 * @type {ScanStream|exports}
 */
exports.Scan = require("./scan.js");

/**
 *
 * @type {BatchWriteStream|exports}
 */
exports.BatchWrite = require("./batchWrite.js");

/**
 * @type {BatchGetStream}
 */
exports.BatchGet = require("./batchGet");