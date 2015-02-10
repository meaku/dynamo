"use strict";

/**
 * helper module for local DynamoDB to be used in tests
 */

var spawn = require('child_process').spawn,
    path = require('path'),
    when = require('when'),
    child;

var defaultOptions = {
    libPath: path.resolve(__dirname, "./jar/DynamoDBLocal_lib"),
    binPath: path.resolve(__dirname, "./jar/DynamoDBLocal.jar"),
    dbPath: path.resolve(__dirname, "./tmp/"),
    debug: true,
    port: 8000
};

/**
 * disable/enable local DynamoDB logs
 * @type {boolean}
 */
exports.debug = false;

/**
 * start a local DynamoDB instance
 *
 * @param {Object=} options pass to overwrite defaultOptions
 * @returns {Promise}
 */
function start(options) {

    options = options || defaultOptions;

    return when.promise(function (resolve, reject) {

        if (child) {
            resolve(child);
            return;
        }

        child = spawn('java', [
            '-Djava.library.path=' + options.libPath,
            '-jar', options.binPath,
            //'-inMemory',
            '-dbPath', options.dbPath,
            '-port', options.port
        ], {});

        child.stderr.on('data', function (msg) {
            msg = msg.toString();

            if (msg.indexOf('Address already in use') !== -1) {
                reject(new Error('Server already running in background'));
                return;
            }

            if (msg.toString().indexOf('AbstractConnector:Started') !== -1) {
                resolve(child);
            }
        });

        child.on("exit", function () {
            console.log("exit", arguments);
        });

        if (exports.debug) {
            child.stderr.pipe(process.stderr);
            child.stdout.pipe(process.stdout);
        }
    }).timeout(2000);
}

/**
 * stop a running local DynamoDB instance
 * @returns {Promise}
 */
function stop() {

    return when.promise(function (resolve, reject) {
        if (!child) {
            reject(new Error("No running instance found"));
        }

        child.on("exit", function () {
            resolve();
            //reset so it can be restarted
            child = null;
        });

        child.kill();
    }).timeout(2000);
}

exports.start = start;
exports.stop = stop;