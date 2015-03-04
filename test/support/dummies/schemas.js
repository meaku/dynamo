var schemas = {
  
  TestTable:{
    // HASH
    "UserId":"S",
    // RANGE
    "FileId":"S",
    // ATTRIBUTES
    "Name":"S",
    "Type":"S",
    "Size":"N",
    "Date":"S",
    "SharedFlag":"S",
    "S3Key":"S",
    ItemsOnMyDesk: "L",
      Pens: "M",
        Quantity: "N",
    "testList":"L",
    "testBoolean":"BOOL",
    "testAttributeString":"S",
    "testAttributeStringSet":"SS",
    "testAttributeNumber":"N",
    "testAttributeNumberSet":"NS",
    "testAttributeBinary":"B",
    "testAttributeBinarySet":"BS"
  }
};

module.exports = schemas;