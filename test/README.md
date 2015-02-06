Testing
=========

# Setup
```bash
sudo npm install mocha
npm install assert.js
```    

# DynamoDB local
Download [Snapshot](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html)
```bash
java -Djava.library.path=./test/db/jar/DynamoDBLocal_lib -jar ./test/db/jar/DynamoDBLocal.jar -inMemory -port 8000
```

# Unit tests
mocha --reporter spec test/unit/*
npm test