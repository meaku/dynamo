"use strict";

/**
 * taken from http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#putItem-property
 *
 * S — (String)
 * A String data type.
 *
 * N — (String)
 * A Number data type.
 *
 * B — (Buffer, Typed Array, Blob, String)
 * A Binary data type.
 *
 * SS — (Array<String>)
 * A String Set data type.
 *
 * NS — (Array<String>)
 * A Number Set data type.
 *
 * BS — (Array<Buffer, Typed Array, Blob, String>)
 * A Binary Set data type.
 *
 * M — (map<map>)
 * A Map of attribute values.
 *
 * L — (Array<map>)
 * A List of attribute values.
 *
 * NULL — (Boolean)
 * A Null data type.
 *
 * BOOL — (Boolean)
 * A Boolean data type.
 *
 */

var castMapping = {
    BOOL: function (val) {
        return Boolean(val);
    },
    S: function (val) {
        return String(val);
    },
    SS: function (val) {
        return val.map(function (v) {
            return String(v);
        });
    },
    NS: function (val) {
        return val.map(function (v) {
            return Number(v);
        });
    },
    BS: function (val) {
        return val.map(function (v) {
            return v.toString("base64");
        });
    },
    N: function (val) {
        return String(val);
    },
    L: function(val) {
        return val.map(function(value) {
            return { S: value };
        });
    },
    M: function(val) {
        var obj  = {};

        Object.keys(val).forEach(function(key) {
           obj[key] = { S: val[key] };
        });

        return obj;
    }
};


function cast(type, value) {
    var castFunction = castMapping[type] || function (val) {
            return val;
        };
    return castFunction(value);
}

function transform(schemas) {

    return {

        to: function (tableName, item) {
            var schema = schemas[tableName] || {};

            function transformTo(key, value) {

                if (key && schema[key]) {
                    var data = {};
                    data[schema[key]] = cast(schema[key], value);

                    return data;
                }

                if (value && typeof value === "object") {
                    var objData = {};

                    Object.keys(value).forEach(function (vKey) {
                        objData[vKey] = transformTo(vKey, value[vKey]);
                    });

                    return objData;
                }

                if (Array.isArray(value)) {
                    return item.map(function (val) {
                        return transformTo(null, val);
                    });
                }

                return {
                    "S": value
                };
            }

            return transformTo(null, item);
        },
        from: function (tableName, item) {
            var schema = schemas[tableName] || {};

            function transformFrom(key, value) {

                if (key && schema[key]) {

                    /**
                     * custom converter back to Object
                     *
                     * //TODO move them to mapping
                     * //TODO currently only supporting string maps & lists
                     * //TODO no deep nesting, max level: 1
                     */

                    if(schema[key] === "N") {
                        return Number(value[schema[key]]);
                    }

                    if(schema[key] === "M") {
                        var data = {};

                        Object.keys(value[schema[key]]).forEach(function(innerKey) {
                            data[innerKey] = value[schema[key]][innerKey]["S"];
                        });

                        return data;
                    }

                    if(schema[key] === "L") {
                        return value[schema[key]].map(function(v) {
                            return v["S"];
                        });
                    }

                    return cast(schema[key], value[schema[key]]);
                }

                if (value && typeof value === "object") {
                    var objData = {};

                    Object.keys(value).forEach(function (vKey) {
                        objData[vKey] = transformFrom(vKey, value[vKey]);
                    });

                    return objData;
                }

                if (Array.isArray(value)) {
                    return item.map(function (val) {
                        return transformFrom(null, val);
                    });
                }
            }

            return transformFrom(null, item);
        }
    };
}

module.exports = transform;