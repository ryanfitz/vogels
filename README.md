# vogels [![Build Status](https://travis-ci.org/ryanfitz/vogels.png?branch=master)](https://travis-ci.org/ryanfitz/vogels)

vogels is a [DynamoDB][5] data mapper for [node.js][1].

## Features
* Simplified data modeling and mapping to DynamoDB types
* Advanced chainable apis for [query](#query) and [scan](#scan) operations
* Data validation
* [Autogenerating UUIDs](#uuid)
* [Global Secondary Indexes](#global-indexes)
* [Local Secondary Indexes](#local-secondary-indexes)
* [Parallel Scans](#parallel-scan)

## Installation

    npm install vogels

## Getting Started
First, you need to configure the [AWS SDK][2] with your credentials.

```js
var vogels = require('vogels');
vogels.AWS.config.loadFromPath('credentials.json');
```

When running on EC2 its recommended to leverage EC2 IAM roles. If you have configured your instance to use IAM roles, Vogels will automatically select these credentials for use in your application, and you do not need to manually provide credentials in any other format.

```js
var vogels = require('vogels');
vogels.AWS.config.update({region: "REGION"}); // region must be set
```

You can also directly pass in your access key id, secret and region.
  * Its recommend you not hard-code credentials inside an application. Use this method only for small personal scripts or for testing purposes.

```js
var vogels = require('vogels');
vogels.AWS.config.update({accessKeyId: 'AKID', secretAccessKey: 'SECRET', region: "REGION"});
```

Currently the following region codes are available in Amazon:

|      Code      |           Name           |
| -------------- | ------------------------ |
| ap-northeast-1 | Asia Pacific (Tokyo)     |
| ap-southeast-1 | Asia Pacific (Singapore) |
| ap-southeast-2 | Asia Pacific (Sydney)    |
| eu-central-1   | EU (Frankfurt)           |
| eu-west-1      | EU (Ireland)             |
| sa-east-1      | South America (Sao Paulo)|
| us-east-1      | US East (N. Virginia)    |
| us-west-1      | US West (N. California)  |
| us-west-2      | US West (Oregon)         |

### Define a Model
Models are defined through the toplevel define method.

```js
var Account = vogels.define('Account', {
  hashKey : 'email',

  // add the timestamp attributes (updatedAt, createdAt)
  timestamps : true,

  schema : {
    email   : Joi.string().email(),
    name    : Joi.string(),
    age     : Joi.number(),
    roles   : vogels.types.stringSet(),
    settings : {
      nickname      : Joi.string(),
      acceptedTerms : Joi.boolean().default(false)
    }
  }
});
```

Models can also be defined with hash and range keys.

```js
var BlogPost = vogels.define('BlogPost', {
  hashKey : 'email',
  rangeKey : ‘title’,
  schema : {
    email   : Joi.string().email(),
    title   : Joi.string(),
    content : Joi.binary(),
    tags   : vogels.types.stringSet(),
  }
});
```

### Schema Types
Vogels provides the following schema types:

* String
* Number
* StringSet
* NumberSet
* Boolean
* Date
* UUID
* TimeUUID

#### UUID
UUIDs can be declared for any attributes, including hash and range keys. By
Default, the uuid will be automatically generated when attempting to create
the model in DynamoDB.

```js
var Tweet = vogels.define('Tweet', {
  hashKey : 'TweetID',
  timestamps : true,
  schema : {
    TweetID : vogels.types.uuid(),
    content : Joi.string(),
  }
});
```

### Configuration
You can configure vogels to automatically add `createdAt` and `updatedAt` timestamp attributes when
saving and updating a model. `updatedAt` will only be set when updating a record and will not be set on initial creation of the model.

```js
var Account = vogels.define('Account', {
  hashKey : 'email',

  // add the timestamp attributes (updatedAt, createdAt)
  timestamps : true,

  schema : {
    email : Joi.string().email(),
  }
});
```

If you want vogels to handle timestamps, but only want some of them, or want your
timestamps to be called something else, you can override each attribute individually:

```js
var Account = vogels.define('Account', {
  hashKey : 'email',

  // enable timestamps support
  timestamps : true,

  // I don't want createdAt
  createdAt: false,

  // I want updatedAt to actually be called updateTimestamp
  updatedAt: 'updateTimestamp'

  schema : {
    email : Joi.string().email(),
  }
});
```

You can override the table name the model will use.

```js
var Event = vogels.define('Event', {
  hashKey : 'name',
  schema : {
    name : Joi.string(),
    total : Joi.number()
  },

  tableName: 'deviceEvents'
});
```

if you set the tableName to a function, vogels will use the result of the function as the active table to use.
Useful for storing time series data.

```js
var Event = vogels.define('Event', {
  hashKey : 'name',
  schema : {
    name : Joi.string(),
    total : Joi.number()
  },

  // store monthly event data
  tableName: function () {
    var d = new Date();
    return ['events', d.getFullYear(), d.getMonth() + 1].join('_');
  }
});
```

After you've defined your model you can configure the table name to use.
By default, the table name used will be the lowercased and pluralized version
of the name you provided when defining the model.

```js
Account.config({tableName: 'AccountsTable'});
```

You can also pass in a custom instance of the aws-sdk DynamoDB client
```js
var dynamodb = new AWS.DynamoDB();
Account.config({dynamodb: dynamodb});

// or globally use custom DynamoDB instance
// all defined models will now use this driver
vogels.dynamoDriver(dynamodb);
```

### Saving Models to DynamoDB
With your models defined, we can start saving them to DynamoDB.

```js
Account.create({email: 'foo@example.com', name: 'Foo Bar', age: 21}, function (err, acc) {
  console.log('created account in DynamoDB', acc.get('email'));
});
```

You can also first instantiate a model and then save it.

```js
var acc = new Account({email: 'test@example.com', name: 'Test Example'});
acc.save(function (err) {
  console.log('created account in DynamoDB', acc.get('email'));
});
```

Saving models that require range and hashkeys are identical to ones with only
hashkeys.

```js
BlogPost.create({
  email: 'werner@example.com', 
  title: 'Expanding the Cloud', 
  content: 'Today, we are excited to announce the limited preview...'
  }, function (err, post) {
    console.log('created blog post', post.get('title'));
  });
```

Pass an array of items and they will be saved in parallel to DynamoDB.

```js
var item1 = {email: 'foo1@example.com', name: 'Foo 1', age: 10};
var item2 = {email: 'foo2@example.com', name: 'Foo 2', age: 20};
var item3 = {email: 'foo3@example.com', name: 'Foo 3', age: 30};

Account.create([item1, item2, item3], function (err, acccounts) {
  console.log('created 3 accounts in DynamoDB', accounts);
});
```

Use expressions api to do conditional writes

```js
  var params = {};
  params.ConditionExpression = '#i <> :x';
  params.ExpressionAttributeNames = {'#i' : 'id'};
  params.ExpressionAttributeValues = {':x' : 123};

  User.create({id : 123, name : 'Kurt Warner' }, params, function (error, acc) { ... });
```

Use the `overwrite` option to prevent over writing of existing records.
  * By default `overwrite` is set to true, allowing create operations to overwrite existing records
```js
  // setting overwrite to false will generate
  // the same Condition Expression as in the previous example
  User.create({id : 123, name : 'Kurt Warner' }, {overwrite : false}, function (error, acc) { ... });
```

### Updating

When updating a model the hash and range key attributes must be given, all
other attributes are optional

```js
// update the name of the foo@example.com account
Account.update({email: 'foo@example.com', name: 'Bar Tester'}, function (err, acc) {
  console.log('update account', acc.get('name'));
});
```

`Model.update` accepts options to pass to DynamoDB when making the updateItem request

```js
Account.update({email: 'foo@example.com', name: 'Bar Tester'}, {ReturnValues: 'ALL_OLD'}, function (err, acc) {
  console.log('update account', acc.get('name')); // prints the old account name
});

// Only update the account if the current age of the account is 21
Account.update({email: 'foo@example.com', name: 'Bar Tester'}, {expected: {age: 22}}, function (err, acc) {
  console.log('update account', acc.get('name'));
});

// setting an attribute to null will delete the attribute from DynamoDB
Account.update({email: 'foo@example.com', age: null}, function (err, acc) {
  console.log('update account', acc.get('age')); // prints null
});
```

You can also pass what action to perform when updating a given attribute
Use $add to increment or decrement numbers and add values to sets

```js
Account.update({email : 'foo@example.com', age : {$add : 1}}, function (err, acc) {
  console.log('incremented age by 1', acc.get('age'));
});

BlogPost.update({
  email : 'werner@example.com',
  title : 'Expanding the Cloud',
  tags  : {$add : 'cloud'}
}, function (err, post) {
  console.log('added single tag to blog post', post.get('tags'));
});

BlogPost.update({
  email : 'werner@example.com',
  title : 'Expanding the Cloud',
  tags  : {$add : ['cloud', 'dynamodb']}
}, function (err, post) {
  console.log('added tags to blog post', post.get('tags'));
});
```

$del will remove values from a given set

```js
BlogPost.update({
  email : 'werner@example.com',
  title : 'Expanding the Cloud',
  tags  : {$del : 'cloud'}
}, function (err, post) {
  console.log('removed cloud tag from blog post', post.get('tags'));
});

BlogPost.update({
  email : 'werner@example.com',
  title : 'Expanding the Cloud',
  tags  : {$del : ['aws', 'node']}
}, function (err, post) {
  console.log('removed multiple tags', post.get('tags'));
});
```

Use the expressions api to update nested documents

```js
var params = {};
  params.UpdateExpression = 'SET #year = #year + :inc, #dir.titles = list_append(#dir.titles, :title), #act[0].firstName = :firstName ADD tags :tag';
  params.ConditionExpression = '#year = :current';
  params.ExpressionAttributeNames = {
    '#year' : 'releaseYear',
    '#dir' : 'director',
    '#act' : 'actors'
  };

  params.ExpressionAttributeValues = {
    ':inc' : 1,
    ':current' : 2001,
    ':title' : ['The Man'],
    ':firstName' : 'Rob',
    ':tag' : vogels.Set(['Sports', 'Horror'], 'S')
  };

Movie.update({title : 'Movie 0', description : 'This is a description'}, params, function (err, mov) {});
```

### Deleting
You delete items in DynamoDB using the hashkey of model
If your model uses both a hash and range key, than both need to be provided

```js
Account.destroy('foo@example.com', function (err) {
  console.log('account deleted');
});

// Destroy model using hash and range key
BlogPost.destroy('foo@example.com', 'Hello World!', function (err) {
  console.log('post deleted')
});

BlogPost.destroy({email: 'foo@example.com', title: 'Another Post'}, function (err) {
  console.log('another post deleted')
});
```

`Model.destroy` accepts options to pass to DynamoDB when making the deleteItem request

```js
Account.destroy('foo@example.com', {ReturnValues: true}, function (err, acc) {
  console.log('account deleted');
  console.log('deleted account name', acc.get('name'));
});

Account.destroy('foo@example.com', {expected: {age: 22}}, function (err) {
  console.log('account deleted if the age was 22');
```


Use expression apis to perform conditional deletes

```js
var params = {};
params.ConditionExpression = '#v = :x';
params.ExpressionAttributeNames = {'#v' : 'version'};
params.ExpressionAttributeValues = {':x' : '2'};

User.destroy({id : 123}, params, function (err, acc) {});
```

### Loading models from DynamoDB
The simpliest way to get an item from DynamoDB is by hashkey.

```js
Account.get('test@example.com', function (err, acc) {
  console.log('got account', acc.get('email'));
});
```

Perform the same get request, but this time peform a consistent read.

```js
Account.get('test@example.com', {ConsistentRead: true}, function (err, acc) {
  console.log('got account', acc.get('email'));
});
```

`Model.get` accepts any options that DynamoDB getItem request supports. For
example:

```js
Account.get('test@example.com', {ConsistentRead: true, AttributesToGet : ['name','age']}, function (err, acc) {
  console.log('got account', acc.get('email'))
  console.log(acc.get('name'));
  console.log(acc.get('age'));
  console.log(acc.get('email')); // prints null
});
```

Get a model using hash and range key.

```js
// load up blog post written by Werner, titled DynamoDB Keeps Getting Better and cheaper
BlogPost.get('werner@example.com', 'dynamodb-keeps-getting-better-and-cheaper', function (err, post) {
  console.log('loaded post by range and hash key', post.get('content'));
});
```

`Model.get` also supports passing an object which contains hash and range key
attributes to load up a model

```js
BlogPost.get({email: 'werner@example.com', title: 'Expanding the Cloud'}, function (err, post) {
  console.log('loded post', post.get('content'));
});
```

Use expressions api to select which attributes you want returned

```js
  User.get({ id : '123456789'},{ ProjectionExpression : 'email, age, settings.nickname' }, function (err, acc) {});
```

### Query
For models that use hash and range keys Vogels provides a flexible and
chainable query api

```js
// query for blog posts by werner@example.com
BlogPost
  .query('werner@example.com')
  .exec(callback);

// same as above, but load all results
BlogPost
  .query('werner@example.com')
  .loadAll()
  .exec(callback);

// only load the first 5 posts by werner
BlogPost
  .query('werner@example.com')
  .limit(5)
  .exec(callback);

// query for posts by werner where the tile begins with 'Expanding'
BlogPost
  .query('werner@example.com')
  .where('title').beginsWith('Expanding')
  .exec(callback);

// return only the count of documents that begin with the title Expanding
BlogPost
  .query('werner@example.com')
  .where('title').beginsWith('Expanding')
  .select('COUNT')
  .exec(callback);

// only return title and content attributes of 10 blog posts
// that begin with the title Expanding
BlogPost
  .query('werner@example.com')
  .where('title').beginsWith('Expanding')
  .attributes(['title', 'content'])
  .limit(10)
  .exec(callback);

// sorting by title ascending
BlogPost
  .query('werner@example.com')
  .ascending()
  .exec(callback)

// sorting by title descending
BlogPost
  .query('werner@example.com')
  .descending()
  .exec(callback)

// All query options are chainable
BlogPost
  .query('werner@example.com')
  .where('title').gt('Expanding')
  .attributes(['title', 'content'])
  .limit(10)
  .ascending()
  .loadAll()
  .exec(callback);
```

Vogels supports all the possible KeyConditions that DynamoDB currently
supports.

```js
BlogPost
  .query('werner@example.com')
  .where('title').equals('Expanding')
  .exec();

// less than equals
BlogPost
  .query('werner@example.com')
  .where('title').lte('Expanding')
  .exec();

// less than
BlogPost
  .query('werner@example.com')
  .where('title').lt('Expanding')
  .exec();

// greater than
BlogPost
  .query('werner@example.com')
  .where('title').gt('Expanding')
  .exec();

// greater than equals
BlogPost
  .query('werner@example.com')
  .where('title').gte('Expanding')
  .exec();

BlogPost
  .query('werner@example.com')
  .where('title').beginsWith('Expanding')
  .exec();

BlogPost
  .query('werner@example.com')
  .where('title').between('foo@example.com', 'test@example.com')
  .exec();
```

Query Filters allow you to further filter results on non-key attributes.

```js
BlogPost
  .query('werner@example.com')
  .where('title').equals('Expanding')
  .filter('tags').contains('cloud')
  .exec();
```

Expression Filters also allow you to further filter results on non-key attributes.

```javascript
BlogPost
  .query('werner@example.com')
  .filterExpression('#title < :t')
  .expressionAttributeValues({ ':t' : 'Expanding' })
  .expressionAttributeNames({ '#title' : 'title'})
  .projectionExpression('#title, tag')
  .exec();
```

See the queryFilter.js [example][0] for more examples of using query filters

#### Global Indexes
First, define a model with a global secondary index.

```js
var GameScore = vogels.define('GameScore', {
  hashKey : 'userId',
  rangeKey : 'gameTitle',
  schema : {
    userId           : Joi.string(),
    gameTitle        : Joi.string(),
    topScore         : Joi.number(),
    topScoreDateTime : Joi.date(),
    wins             : Joi.number(),
    losses           : Joi.number()
  },
  indexes : [{
    hashKey : 'gameTitle', rangeKey : 'topScore', name : 'GameTitleIndex', type : 'global'
  }]
});
```

Now we can query against the global index 

```js
GameScore
  .query('Galaxy Invaders')
  .usingIndex('GameTitleIndex')
  .descending()
  .exec(callback);
```

When can also configure the attributes projected into the index.
By default all attributes will be projected when no Projection pramater is
present 

```js
var GameScore = vogels.define('GameScore', {
  hashKey : 'userId',
  rangeKey : 'gameTitle',
  schema : {
    userId           : Joi.string(),
    gameTitle        : Joi.string(),
    topScore         : Joi.number(),
    topScoreDateTime : Joi.date(),
    wins             : Joi.number(),
    losses           : Joi.number()
  },
  indexes : [{
    hashKey : 'gameTitle',
    rangeKey : 'topScore',
    name : 'GameTitleIndex',
    type : 'global',
    projection: { NonKeyAttributes: [ 'wins' ], ProjectionType: 'INCLUDE' } //optional, defaults to ALL

  }]
});
```

Filter items against the configured rangekey for the global index.

```js
GameScore
  .query('Galaxy Invaders')
  .usingIndex('GameTitleIndex')
  .where('topScore').gt(1000)
  .descending()
  .exec(function (err, data) {
    console.log(_.map(data.Items, JSON.stringify));
  });
```

#### Local Secondary Indexes
First, define a model using a local secondary index

```js
var BlogPost = vogels.define('Account', {
  hashKey : 'email',
  rangekey : 'title',
  schema : {
    email             : Joi.string().email(),
    title             : Joi.string(),
    content           : Joi.binary(),
    PublishedDateTime : Joi.date()
  },

  indexes : [{
    hashkey : 'email', rangekey : 'PublishedDateTime', type : 'local', name : 'PublishedIndex'
  }]
});
```

Now we can query for blog posts using the secondary index

```js
BlogPost
  .query('werner@example.com')
  .usingIndex('PublishedIndex')
  .descending()
  .exec(callback);
```

Could also query for published posts, but this time return oldest first

```js
BlogPost
  .query('werner@example.com')
  .usingIndex('PublishedIndex')
  .ascending()
  .exec(callback);
```

Finally lets load all published posts sorted by publish date
```js
BlogPost
  .query('werner@example.com')
  .usingIndex('PublishedIndex')
  .descending()
  .loadAll()
  .exec(callback);
```

Learn more about [secondary indexes][3]

### Scan
Vogels provides a flexible and chainable api for scanning over all your items
This api is very similar to the query api.

```js
// scan all accounts, returning the first page or results
Account.scan().exec(callback);

// scan all accounts, this time loading all results
// note this will potentially make several calls to DynamoDB 
// in order to load all results
Account
  .scan()
  .loadAll()
  .exec(callback);

// Load 20 accounts
Account
  .scan()
  .limit(20)
  .exec();

// Load All accounts, 20 at a time per request
Account
  .scan()
  .limit(20)
  .loadAll()
  .exec();

// Load accounts which match a filter
// only return email and created attributes
// and return back the consumed capacity the request took
Account
  .scan()
  .where('email').gte('f@example.com')
  .attributes(['email','created'])
  .returnConsumedCapacity()
  .exec();

// Returns number of matching accounts, rather than the matching accounts themselves
Account
  .scan()
  .where('age').gte(21)
  .select('COUNT')
  .exec();

// Start scan using start key
Account
  .scan()
  .where('age').notNull()
  .startKey('foo@example.com')
  .exec()
```

Vogels supports all the possible Scan Filters that DynamoDB currently supports.

```js
// equals
Account
  .scan()
  .where('name').equals('Werner')
  .exec();

// not equals
Account
  .scan()
  .where('name').ne('Werner')
  .exec();

// less than equals
Account
  .scan()
  .where('name').lte('Werner')
  .exec();

// less than
Account
  .scan()
  .where('name').lt('Werner')
  .exec();

// greater than equals
Account
  .scan()
  .where('name').gte('Werner')
  .exec();

// greater than
Account
  .scan()
  .where('name').gt('Werner')
  .exec();

// name attribute doesn't exist
Account
  .scan()
  .where('name').null()
  .exec();

// name attribute exists
Account
  .scan()
  .where('name').notNull()
  .exec();

// contains
Account
  .scan()
  .where('name').contains('ner')
  .exec();

// not contains
Account
  .scan()
  .where('name').notContains('ner')
  .exec();

// in
Account
  .scan()
  .where('name').in(['foo@example.com', 'bar@example.com'])
  .exec();

// begins with
Account
  .scan()
  .where('name').beginsWith('Werner')
  .exec();

// between
Account
  .scan()
  .where('name').between('Bar', 'Foo')
  .exec();

// multiple filters
Account
  .scan()
  .where('name').equals('Werner')
  .where('age').notNull()
  .exec();
```

You can also use the new expressions api when filtering scans

```javascript
User.scan()
  .filterExpression('#age BETWEEN :low AND :high AND begins_with(#email, :e)')
  .expressionAttributeValues({ ':low' : 18, ':high' : 22, ':e' : 'test1'})
  .expressionAttributeNames({ '#age' : 'age', '#email' : 'email'})
  .projectionExpression('#age, #email')
  .exec();
```

### Parallel Scan
Parallel scans increase the throughput of your table scans.
The parallel scan operation is identical to the scan api.
The only difference is you must provide the total number of segments

**Caution** you can easily consume all your provisioned throughput with this api

```js
var totalSegments = 8;

Account.parallelScan(totalSegments)
  .where('age').gte(18)
  .attributes('age')
  .exec(callback);

// Load All accounts
Account
  .parallelScan(totalSegments)
  .exec()
```

More info on [Parallel Scans][4]

### Batch Get Items
`Model.getItems` allows you to load multiple models with a single request to DynamoDB.

DynamoDB limits the number of items you can get to 100 or 1MB of data for a single request.
Vogels automatically handles splitting up into multiple requests to load all
items.

```js
Account.getItems(['foo@example.com','bar@example.com', 'test@example.com'], function (err, accounts) {
  console.log('loaded ' + accounts.length + ' accounts'); // prints loaded 3 accounts
});

// For models with range keys you must pass in objects of hash and range key attributes
var postKey1 = {email : 'test@example.com', title : 'Hello World!'};
var postKey2 = {email : 'test@example.com', title : 'Another Post'};

BlogPost.getItems([postKey1, postKey2], function (err, posts) {
  console.log('loaded posts');
});
```

`Model.getItems` accepts options which will be passed to DynamoDB when making the batchGetItem request

```js
// Get both accounts, using a consistent read
Account.getItems(['foo@example.com','bar@example.com'], {ConsistentRead: true}, function (err, accounts) {
  console.log('loaded ' + accounts.length + ' accounts'); // prints loaded 2 accounts
});
```

### Streaming api
vogels supports a basic streaming api in addition to the callback
api for `query`, `scan`, and `parallelScan` operations.

```js
var stream = Account.parallelScan(4).exec();

stream.on('readable', function () {
  console.log('single parallel scan response', stream.read());
});

stream.on('end', function () {
  console.log('Parallel scan of accounts finished');
});

var querystream = BlogPost.query('werner@vogels.com').loadAll().exec();

querystream.on('readable', function () {
  console.log('single query response', stream.read());
});

querystream.on('end', function () {
  console.log('query for blog posts finished');
});
```

### Dynamic Table Names
vogels supports dynamic table names, useful for storing time series data.

```js
var Event = vogels.define('Event', {
  hashKey : 'name',
  schema : {
    name : Joi.string(),
    total : Joi.number()
  },

  // store monthly event data
  tableName: function () {
    var d = new Date();
    return ['events', d.getFullYear(), d.getMonth() + 1].join('_');
  }
});
```

### Logging
Logging can be enabled to provide detailed information on data being sent and returned from DynamoDB.
By default logging is turned off.

```js
vogels.log.level('info'); // enabled INFO log level 
```

Logging can also be enabled / disabled at the model level.

```js
var Account = vogels.define('Account', {hashKey : 'email'});
var Event = vogels.define('Account', {hashKey : 'name'});

Account.log.level('warn'); // enable WARN log level for Account model operations
```

## Examples

```js
var vogels = require('vogels');

var Account = vogels.define('Account', {
  hashKey : 'email',

  // add the timestamp attributes (updatedAt, createdAt)
  timestamps : true,

  schema : {
    email   : Joi.string().email(),
    name    : Joi.string().required(),
    age     : Joi.number(),
  }
});

Account.create({email: 'test@example.com', name : 'Test Account'}, function (err, acc) {
  console.log('created account at', acc.get('created')); // prints created Date

  acc.set({age: 22});

  acc.update(function (err) {
    console.log('updated account age');
  });

});
```

See the [examples][0] for more working sample code.

### Support

Vogels is provided as-is, free of charge. For support, you have a few choices:

- Ask your support question on [Stackoverflow.com](http://stackoverflow.com), and tag your question with **vogels**.
- If you believe you have found a bug in vogels, please submit a support ticket on the [Github Issues page for vogels](http://github.com/ryanfitz/vogels/issues). We'll get to them as soon as we can.
- For general feedback message me on [twitter](https://twitter.com/theryanfitz)
- For more personal or immediate support, I’m available for hire to consult on your project. [Contact](mailto:ryan.fitz1@gmail.com) me for more detals.

### Maintainers

- [Ryan Fitzgerald](http://github.com/ryanfitz) ([@ryanfitz](https://twitter.com/theryanfitz))

### License

(The MIT License)

Copyright (c) 2015 Ryan Fitzgerald

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

[0]: https://github.com/ryanfitz/vogels/tree/master/examples
[1]: http://nodejs.org
[2]: http://aws.amazon.com/sdkfornodejs
[3]: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LSI.html
[4]: http://aws.typepad.com/aws/2013/05/amazon-dynamodb-parallel-scans-and-other-good-news.html
[5]: http://aws.amazon.com/dynamodb
