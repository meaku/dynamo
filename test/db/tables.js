 var tables = {
   
  "TestTable": { 
      AttributeDefinitions: [
        { AttributeName: "UserId", AttributeType: "S" },
        { AttributeName: "FileId", AttributeType: "S" },
        { AttributeName: "Type", AttributeType: "S" }
      ],
      KeySchema: [ // required
        { AttributeName: 'UserId', KeyType: 'HASH'},
        { AttributeName: 'FileId', KeyType: 'RANGE'}
      ],
      ProvisionedThroughput: { // required
        ReadCapacityUnits: 10, // required
        WriteCapacityUnits: 10 // required
      },
      TableName: 'TestTable', // required
      GlobalSecondaryIndexes: [
        {
          IndexName: 'TypeIndex', // required
          KeySchema: [ // required
            { AttributeName: 'UserId', KeyType: 'HASH'},
            { AttributeName: 'Type', KeyType: 'RANGE'}
            // ... more items ...
          ],
          Projection: { // required
            ProjectionType: 'ALL'
          },
          ProvisionedThroughput: { // required
            ReadCapacityUnits: 1, // required
            WriteCapacityUnits: 1 // required
          }
        }
      ]   
    }
};

module.exports = tables;