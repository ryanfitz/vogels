'use strict';

var helper = require('./test-helper'),
    Schema = require('../lib/schema'),
    Scan   = require('../lib/scan');

describe('Scan', function () {
  var schema,
      serializer,
      table;

  beforeEach(function () {
    schema = new Schema();
    serializer = helper.mockSerializer(),

    table = helper.mockTable();
    table.tableName = function () {
      return 'accounts';
    };

    table.schema = schema;
  });

  describe('#exec', function () {

    it('should call run scan on table', function (done) {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});

      table.runScan.yields(null, {ConsumedCapacity: {CapacityUnits : 5, TableName: 'accounts'}, Count: 10, ScannedCount: 12});
      serializer.serializeItem.returns({name: {S: 'tim'}});

      new Scan(table, serializer).exec(function (err, results) {
        results.ConsumedCapacity.should.eql({CapacityUnits: 5, TableName: 'accounts'});
        results.Count.should.equal(10);
        results.ScannedCount.should.equal(12);

        done();
      });
    });

    it('should return LastEvaluatedKey', function (done) {
      schema.String('name', {hashKey: true});
      schema.String('email');

      table.runScan.yields(null, {LastEvaluatedKey: {name : 'tim'}, Count: 10, ScannedCount: 12});
      serializer.serializeItem.returns({name: {S: 'tim'}});

      new Scan(table, serializer).exec(function (err, results) {
        results.Count.should.equal(10);
        results.ScannedCount.should.equal(12);

        results.LastEvaluatedKey.should.eql({name : 'tim'});

        done();
      });
    });

  });

  describe('#limit', function () {

    it('should set the limit', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});

      var scan = new Scan(table, serializer).limit(10);

      scan.request.Limit.should.equal(10);
    });

  });

  describe('#scan', function () {

    it('should set array attributes to get', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.Date('created', {secondaryIndex: true});

      var scan = new Scan(table, serializer).attributes(['created']);
      scan.request.AttributesToGet.should.eql(['created']);
    });

    it('should set single attribute to get', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.Date('created', {secondaryIndex: true});

      var scan = new Scan(table, serializer).attributes('email');
      scan.request.AttributesToGet.should.eql(['email']);
    });

  });

  describe('#startKey', function () {
    it('should set start Key to hash', function () {
      schema.String('name', {hashKey: true});
      schema.String('email');

      var key = {name: {S: 'tim'}};
      serializer.buildKey.returns(key);

      var scan = new Scan(table, serializer).startKey('tim');

      scan.request.ExclusiveStartKey.should.eql(key);
    });

    it('should set start Key to hash + range', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.Date('created', {secondaryIndex: true});

      var key = {name: {S: 'tim'}, email : {S: 'foo@example.com'}};
      serializer.buildKey.returns(key);

      var scan = new Scan(table, serializer).startKey({name: 'tim', email: 'foo@example.com'});

      scan.request.ExclusiveStartKey.should.eql(key);
    });
  });

  describe('#select', function () {

    it('should set select Key', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.Date('created', {secondaryIndex: true});

      var scan = new Scan(table, serializer).select('COUNT');

      scan.request.Select.should.eql('COUNT');
    });
  });

  describe('#ReturnConsumedCapacity', function () {

    it('should set return consumed capacity Key to passed in value', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.Date('created', {secondaryIndex: true});

      var scan = new Scan(table, serializer).returnConsumedCapacity('TOTAL');
      scan.request.ReturnConsumedCapacity.should.eql('TOTAL');
    });

    it('should set return consumed capacity Key', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.Date('created', {secondaryIndex: true});

      var scan = new Scan(table, serializer).returnConsumedCapacity();

      scan.request.ReturnConsumedCapacity.should.eql('TOTAL');
    });
  });

  describe('#segment', function () {

    it('should set both segment and total segments keys', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.Date('created', {secondaryIndex: true});

      var scan = new Scan(table, serializer).segments(0, 4);

      scan.request.Segment.should.eql(0);
      scan.request.TotalSegments.should.eql(4);
    });
  });


  describe('#where', function () {
    var scan;

    beforeEach(function () {
      scan = new Scan(table, serializer);

      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.Date('created', {secondaryIndex: true});
      schema.NumberSet('scores');
    });

    it('should have equals clause', function() {
      serializer.serializeItem.returns({email: {S: 'foo@example.com'}});

      scan = scan.where('email').equals('foo@example.com');

      scan.request.ScanFilter.email.should.eql({AttributeValueList: [{S: 'foo@example.com'}], ComparisonOperator: 'EQ'});
    });

    it('should have not equals clause', function() {
      serializer.serializeItem.returns({email: {S: 'foo@example.com'}});

      scan = scan.where('email').ne('foo@example.com');

      scan.request.ScanFilter.email.should.eql({AttributeValueList: [{S: 'foo@example.com'}], ComparisonOperator: 'NE'});
    });

    it('should have less than or equal clause', function() {
      serializer.serializeItem.returns({email: {S: 'foo@example.com'}});

      scan = scan.where('email').lte('foo@example.com');

      scan.request.ScanFilter.email.should.eql({AttributeValueList: [{S: 'foo@example.com'}], ComparisonOperator: 'LE'});
    });

    it('should have less than clause', function() {
      serializer.serializeItem.returns({email: {S: 'foo@example.com'}});

      scan = scan.where('email').lt('foo@example.com');

      scan.request.ScanFilter.email.should.eql({AttributeValueList: [{S: 'foo@example.com'}], ComparisonOperator: 'LT'});
    });

    it('should have greater than or equal clause', function() {
      serializer.serializeItem.returns({email: {S: 'foo@example.com'}});

      scan = scan.where('email').gte('foo@example.com');

      scan.request.ScanFilter.email.should.eql({AttributeValueList: [{S: 'foo@example.com'}], ComparisonOperator: 'GE'});
    });

    it('should have greater than clause', function() {
      serializer.serializeItem.returns({email: {S: 'foo@example.com'}});

      scan = scan.where('email').gt('foo@example.com');

      scan.request.ScanFilter.email.should.eql({AttributeValueList: [{S: 'foo@example.com'}], ComparisonOperator: 'GT'});
    });

    it('should have not null clause', function() {
      scan = scan.where('email').notNull();

      scan.request.ScanFilter.email.should.eql({ComparisonOperator: 'NOT_NULL'});
    });

    it('should have null clause', function() {
      scan = scan.where('email').null();

      scan.request.ScanFilter.email.should.eql({ComparisonOperator: 'NULL'});
    });

    it('should have contains clause', function() {
      serializer.serializeItem.returns({email: {S: 'foo@example.com'}});
      scan = scan.where('email').contains('foo@example.com');

      scan.request.ScanFilter.email.should.eql({AttributeValueList: [{S: 'foo@example.com'}], ComparisonOperator: 'CONTAINS'});
    });

    it('should not pass a number set when making contains call', function() {
      serializer.serializeItem.withArgs(schema, {scores: 2}, {convertSets: true}).returns({scores: {N: '2'}});
      scan = scan.where('scores').contains(2);

      scan.request.ScanFilter.scores.should.eql({AttributeValueList: [{N: '2'}], ComparisonOperator: 'CONTAINS'});
    });

    it('should have not contains clause', function() {
      serializer.serializeItem.returns({email: {S: 'foo@example.com'}});
      scan = scan.where('email').notContains('foo@example.com');

      scan.request.ScanFilter.email.should.eql({AttributeValueList: [{S: 'foo@example.com'}], ComparisonOperator: 'NOT_CONTAINS'});
    });

    it('should have in clause', function() {
      serializer.serializeItem.withArgs(schema, {email: 'foo@example.com'}).returns({email: {S: 'foo@example.com'}});
      serializer.serializeItem.withArgs(schema, {email: 'test@example.com'}).returns({email: {S: 'test@example.com'}});

      scan = scan.where('email').in(['foo@example.com', 'test@example.com']);

      scan.request.ScanFilter.email.should.eql({
        AttributeValueList: [{S: 'foo@example.com'}, {S: 'test@example.com'}],
        ComparisonOperator: 'IN'
      });
    });

    it('should have begins with clause', function() {
      serializer.serializeItem.returns({email: {S: 'foo'}});

      scan = scan.where('email').beginsWith('foo');

      scan.request.ScanFilter.email.should.eql({AttributeValueList: [{S: 'foo'}], ComparisonOperator: 'BEGINS_WITH'});
    });

    it('should have between clause', function() {
      serializer.serializeItem.withArgs(schema, {email: 'bob@bob.com'}).returns({email: {S: 'bob@bob.com'}});
      serializer.serializeItem.withArgs(schema, {email: 'foo@foo.com'}).returns({email: {S: 'foo@foo.com'}});

      scan = scan.where('email').between(['bob@bob.com', 'foo@foo.com']);

      var expect = {
        AttributeValueList: [
          {S: 'bob@bob.com'},
          {S: 'foo@foo.com'}
        ],
        ComparisonOperator: 'BETWEEN'
      };

      scan.request.ScanFilter.email.should.eql(expect);
    });

    it('should have multiple filters', function() {
      serializer.serializeItem.withArgs(schema, {email: 'foo'}).returns({email: {S: 'foo'}});
      serializer.serializeItem.withArgs(schema, {name: 'Tim'}).returns({name: {S: 'Tim'}});

      scan = scan
        .where('name').equals('Tim')
        .where('email').beginsWith('foo');

      var expect = {
        name  : {AttributeValueList: [{S: 'Tim'}], ComparisonOperator: 'EQ'},
        email : {AttributeValueList: [{S: 'foo'}], ComparisonOperator: 'BEGINS_WITH'}
      };

      scan.request.ScanFilter.should.eql(expect);
    });

  });

  describe('#loadAll', function () {

    it('should set load all option to true', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});

      var scan = new Scan(table, serializer).limit(10);

      scan.options.loadAll = true;
    });
  });


});
