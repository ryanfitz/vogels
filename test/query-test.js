'use strict';

var helper = require('./test-helper'),
    Schema = require('../lib/schema'),
    Query  = require('../lib//query');

describe('Query', function () {
  var schema,
      serializer,
      table;

  beforeEach(function () {
    schema = new Schema();
    serializer = helper.mockSerializer(),

    table = helper.mockTable();
    table.config = {name : 'accounts'};
    table.schema = schema;
  });

  describe('#exec', function () {

    it('should run query against table', function (done) {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});

      table.runQuery.yields(null, {});
      serializer.serializeItem.returns({name: {S: 'tim'}});

      new Query('tim', table, serializer).exec(function (err, results) {
        results.should.eql({Items: [], Count: 0});
        done();
      });
    });

  });

  describe('#limit', function () {

    it('should set the limit', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});

      var query = new Query('tim', table, serializer).limit(10);

      query.request.Limit.should.equal(10);
    });

  });

  describe('#usingIndex', function () {

    it('should set the index name to use', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.Date('created', {secondaryIndex: true});

      var query = new Query('tim', table, serializer).usingIndex('created');

      query.request.IndexName.should.equal('created');
    });

    it('should create key condition for global index hash key', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.Number('age');

      schema.globalIndex('UserAgeIndex', {hashKey: 'age'});

      serializer.serializeItem.returns({age: {N: '18'}});

      var query = new Query(18, table, serializer).usingIndex('UserAgeIndex');
      query.buildRequest();

      query.request.IndexName.should.equal('UserAgeIndex');
      query.request.KeyConditions.age.should.eql({AttributeValueList: [{N: '18'}], ComparisonOperator: 'EQ'});
    });
  });

  describe('#consistentRead', function () {

    it('should set Consistent Read to true', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.Date('created', {secondaryIndex: true});

      var query = new Query('tim', table, serializer).consistentRead(true);
      query.request.ConsistentRead.should.be.true;
    });

    it('should set Consistent Read to false', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.Date('created', {secondaryIndex: true});

      var query = new Query('tim', table, serializer).consistentRead(false);
      query.request.ConsistentRead.should.be.false;
    });

  });

  describe('#attributes', function () {

    it('should set array attributes to get', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.Date('created', {secondaryIndex: true});

      var query = new Query('tim', table, serializer).attributes(['created']);
      query.request.AttributesToGet.should.eql(['created']);
    });

    it('should set single attribute to get', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.Date('created', {secondaryIndex: true});

      var query = new Query('tim', table, serializer).attributes('email');
      query.request.AttributesToGet.should.eql(['email']);
    });

  });

  describe('#order', function () {

    it('should set scan index forward to true', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.Date('created', {secondaryIndex: true});

      var query = new Query('tim', table, serializer).ascending();
      query.request.ScanIndexForward.should.be.true;
    });

    it('should set scan index forward to false', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.Date('created', {secondaryIndex: true});

      var query = new Query('tim', table, serializer).descending();
      query.request.ScanIndexForward.should.be.false;
    });

  });

  describe('#startKey', function () {

    it('should set start Key', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.Date('created', {secondaryIndex: true});

      var key = {name: {S: 'tim'}, email : {S: 'foo@example.com'}};
      serializer.buildKey.returns(key);

      var query = new Query('tim', table, serializer).startKey({name: 'tim', email: 'foo@example.com'});

      query.request.ExclusiveStartKey.should.eql(key);
    });
  });

  describe('#select', function () {

    it('should set select Key', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.Date('created', {secondaryIndex: true});

      var query = new Query('tim', table, serializer).select('COUNT');

      query.request.Select.should.eql('COUNT');
    });
  });

  describe('#ReturnConsumedCapacity', function () {

    it('should set return consumed capacity Key to passed in value', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.Date('created', {secondaryIndex: true});

      var query = new Query('tim', table, serializer).returnConsumedCapacity('TOTAL');
      query.request.ReturnConsumedCapacity.should.eql('TOTAL');
    });

    it('should set return consumed capacity Key', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.Date('created', {secondaryIndex: true});

      var query = new Query('tim', table, serializer).returnConsumedCapacity();

      query.request.ReturnConsumedCapacity.should.eql('TOTAL');
    });
  });

  describe('#where', function () {
    var query;

    beforeEach(function () {
      query = new Query('tim', table, serializer);

      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.Date('created', {secondaryIndex: true});
    });

    it('should have equals clause', function() {
      serializer.serializeItem.returns({email: {S: 'foo@example.com'}});

      query = query.where('email').equals('foo@example.com');

      query.request.KeyConditions.email.should.eql({AttributeValueList: [{S: 'foo@example.com'}], ComparisonOperator: 'EQ'});
    });

    it('should have less than or equal clause', function() {
      serializer.serializeItem.returns({email: {S: 'foo@example.com'}});

      query = query.where('email').lte('foo@example.com');

      query.request.KeyConditions.email.should.eql({AttributeValueList: [{S: 'foo@example.com'}], ComparisonOperator: 'LE'});
    });

    it('should have less than clause', function() {
      serializer.serializeItem.returns({email: {S: 'foo@example.com'}});

      query = query.where('email').lt('foo@example.com');

      query.request.KeyConditions.email.should.eql({AttributeValueList: [{S: 'foo@example.com'}], ComparisonOperator: 'LT'});
    });

    it('should have greater than or equal clause', function() {
      serializer.serializeItem.returns({email: {S: 'foo@example.com'}});

      query = query.where('email').gte('foo@example.com');

      query.request.KeyConditions.email.should.eql({AttributeValueList: [{S: 'foo@example.com'}], ComparisonOperator: 'GE'});
    });

    it('should have greater than clause', function() {
      serializer.serializeItem.returns({email: {S: 'foo@example.com'}});

      query = query.where('email').gt('foo@example.com');

      query.request.KeyConditions.email.should.eql({AttributeValueList: [{S: 'foo@example.com'}], ComparisonOperator: 'GT'});
    });

    it('should have begins with clause', function() {
      serializer.serializeItem.returns({email: {S: 'foo'}});

      query = query.where('email').beginsWith('foo');

      query.request.KeyConditions.email.should.eql({AttributeValueList: [{S: 'foo'}], ComparisonOperator: 'BEGINS_WITH'});
    });

    it('should have between clause', function() {
      serializer.serializeItem.withArgs(schema, {email: 'bob@bob.com'}).returns({email: {S: 'bob@bob.com'}});
      serializer.serializeItem.withArgs(schema, {email: 'foo@foo.com'}).returns({email: {S: 'foo@foo.com'}});

      query = query.where('email').between(['bob@bob.com', 'foo@foo.com']);

      var expect = {
        AttributeValueList: [
          {S: 'bob@bob.com'},
          {S: 'foo@foo.com'}
        ],
        ComparisonOperator: 'BETWEEN'
      };

      query.request.KeyConditions.email.should.eql(expect);
    });

  });

  describe('#filter', function () {
    var query;

    beforeEach(function () {
      query = new Query('tim', table, serializer);

      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.Date('created', {secondaryIndex: true});
      schema.Number('age');
    });

    it('should have equals clause', function() {
      serializer.serializeItem.withArgs(schema, {age: 5}).returns({age: {N: '5'}});

      query = query.filter('age').equals(5);

      query.request.QueryFilter.age.should.eql({AttributeValueList: [{N: '5'}], ComparisonOperator: 'EQ'});
    });

    it('should have exists clause', function() {
      query = query.filter('age').exists();

      query.request.QueryFilter.age.should.eql({ComparisonOperator: 'NOT_NULL'});
    });

    it('should have not exists clause', function() {
      query = query.filter('age').exists(false);

      query.request.QueryFilter.age.should.eql({ComparisonOperator: 'NULL'});
    });

    it('should have between clause', function() {
      serializer.serializeItem.withArgs(schema, {age: 5}).returns({age: {N: '5'}});
      serializer.serializeItem.withArgs(schema, {age: 7}).returns({age: {N: '7'}});

      query = query.filter('age').between([5, 7]);

      var expected = {
        AttributeValueList: [
          {N: '5'},
          {N: '7'}
        ],
        ComparisonOperator: 'BETWEEN'
      };

      query.request.QueryFilter.age.should.eql(expected);
    });

    it('should have IN clause', function() {
      serializer.serializeItem.withArgs(schema, {age: 5}).returns({age: {N: '5'}});
      serializer.serializeItem.withArgs(schema, {age: 7}).returns({age: {N: '7'}});

      query = query.filter('age').in([5, 7]);

      var expected = {
        AttributeValueList: [
          {N: '5'},
          {N: '7'}
        ],
        ComparisonOperator: 'IN'
      };

      query.request.QueryFilter.age.should.eql(expected);
    });

  });

  describe('#loadAll', function () {

    it('should set load all option to true', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});

      var query = new Query('tim', table, serializer).loadAll();

      query.options.loadAll = true;
    });
  });

});
