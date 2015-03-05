"use strict";

var dataTypes = require("dynamodb-data-types");

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

exports.to = function(item) {
  return dataTypes.AttributeValue.wrap(item);
};

exports.from = function(item) {
  return dataTypes.AttributeValue.unwrap(item);
};