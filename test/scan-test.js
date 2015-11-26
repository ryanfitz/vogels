'use strict';

var helper = require('./test-helper'),
    Schema = require('../lib/schema'),
    Scan   = require('../lib/scan'),
    _      = require('lodash'),
    chai   = require('chai'),
    expect = chai.expect,
    Joi    = require('joi');

chai.should();

var internals = {};

internals.assertScanFilter = function (scan, expected) {
  var conds = _.map(scan.request.ScanFilter, function (c) {
    return c.format();
  });

  if(!_.isArray(expected)) {
    expected = [expected];
  }

  conds.should.eql(expected);
};

describe('Scan', function () {
  var schema,
      serializer,
      table;

  beforeEach(function () {
    serializer = helper.mockSerializer(),

    table = helper.mockTable();
    table.tableName = function () {
      return 'accounts';
    };

    table.docClient = helper.mockDocClient();

    var config = {
      hashKey: 'name',
      rangeKey: 'email',
      schema : {
        name : Joi.string(),
        email : Joi.string(),
        created : Joi.date()
      },
      indexes : [{ hashKey : 'name', rangeKey : 'created', type : 'local', name : 'CreatedIndex'}]
    };

    schema = new Schema(config);
    table.schema = schema;
  });

  describe('#exec', function () {

    it('should call run scan on table', function (done) {
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
      table.runScan.yields(null, {LastEvaluatedKey: {name : 'tim'}, Count: 10, ScannedCount: 12});
      serializer.serializeItem.returns({name: {S: 'tim'}});

      new Scan(table, serializer).exec(function (err, results) {
        results.Count.should.equal(10);
        results.ScannedCount.should.equal(12);

        results.LastEvaluatedKey.should.eql({name : 'tim'});

        done();
      });
    });

    it('should return error', function (done) {
      table.runScan.yields(new Error('Fail'));

      new Scan(table, serializer).exec(function (err, results) {
        expect(err).to.exist;
        expect(results).to.not.exist;
        done();
      });
    });

    it('should run scan after encountering a retryable exception', function (done) {
      var err = new Error('RetryableException');
      err.retryable = true;

      table.runScan
        .onCall(0).yields(err)
        .onCall(1).yields(err)
        .onCall(2).yields(null, {Items : [{name : 'foo'}]});

      new Scan(table, serializer).exec(function (err, data) {
        expect(err).to.not.exist;
        expect(data).to.exist;
        expect(data.Items).to.have.length(1);

        expect(table.runScan.calledThrice).to.be.true;
        done();
      });
    });


  });

  describe('#limit', function () {

    it('should set the limit', function () {
      var scan = new Scan(table, serializer).limit(10);

      scan.request.Limit.should.equal(10);
    });


    it('should throw when limit is zero', function () {
      var scan = new Scan(table, serializer);
      expect(function () {
        scan.limit(0);
      }).to.throw('Limit must be greater than 0');
    });

  });

  describe('#attributes', function () {

    it('should set array attributes to get', function () {
      var scan = new Scan(table, serializer).attributes(['created', 'email']);
      scan.request.ProjectionExpression.should.eql('#created,#email');
      scan.request.ExpressionAttributeNames.should.eql({'#created' : 'created', '#email' : 'email'});
    });

    it('should set single attribute to get', function () {
      var scan = new Scan(table, serializer).attributes('email');
      scan.request.ProjectionExpression.should.eql('#email');
      scan.request.ExpressionAttributeNames.should.eql({'#email' : 'email'});
    });

  });

  describe('#startKey', function () {
    it('should set start Key to hash', function () {
      var key = {name: {S: 'tim'}};
      serializer.buildKey.returns(key);

      var scan = new Scan(table, serializer).startKey('tim');

      scan.request.ExclusiveStartKey.should.eql(key);
    });

    it('should set start Key to hash + range', function () {
      var key = {name: {S: 'tim'}, email : {S: 'foo@example.com'}};
      serializer.buildKey.returns(key);

      var scan = new Scan(table, serializer).startKey({name: 'tim', email: 'foo@example.com'});

      scan.request.ExclusiveStartKey.should.eql(key);
    });
  });

  describe('#select', function () {

    it('should set select Key', function () {
      var scan = new Scan(table, serializer).select('COUNT');

      scan.request.Select.should.eql('COUNT');
    });
  });

  describe('#ReturnConsumedCapacity', function () {

    it('should set return consumed capacity Key to passed in value', function () {
      var scan = new Scan(table, serializer).returnConsumedCapacity('TOTAL');
      scan.request.ReturnConsumedCapacity.should.eql('TOTAL');
    });

    it('should set return consumed capacity Key', function () {
      var scan = new Scan(table, serializer).returnConsumedCapacity();

      scan.request.ReturnConsumedCapacity.should.eql('TOTAL');
    });
  });

  describe('#segment', function () {

    it('should set both segment and total segments keys', function () {
      var scan = new Scan(table, serializer).segments(0, 4);

      scan.request.Segment.should.eql(0);
      scan.request.TotalSegments.should.eql(4);
    });
  });


  describe('#where', function () {
    var scan;

    beforeEach(function () {

      var config = {
        hashKey: 'name',
        rangeKey: 'email',
        schema : {
          name : Joi.string(),
          email : Joi.string(),
          created : Joi.date(),
          scores : Schema.types.numberSet()
        },
        indexes : [{ hashKey : 'name', rangeKey : 'created', type : 'local', name : 'CreatedIndex'}]
      };

      schema = new Schema(config);
      table.schema = schema;

      scan = new Scan(table, serializer);
    });

    it('should have equals clause', function() {
      scan = scan.where('email').equals('foo@example.com');

      scan.request.ExpressionAttributeNames.should.eql({'#email' : 'email'});
      scan.request.ExpressionAttributeValues.should.eql({':email' : 'foo@example.com'});
      scan.request.FilterExpression.should.eql('(#email = :email)');
    });

    it('should have not equals clause', function() {
      scan = scan.where('email').ne('foo@example.com');

      scan.request.ExpressionAttributeNames.should.eql({'#email' : 'email'});
      scan.request.ExpressionAttributeValues.should.eql({':email' : 'foo@example.com'});
      scan.request.FilterExpression.should.eql('(#email <> :email)');
    });

    it('should have less than or equal clause', function() {
      scan = scan.where('email').lte('foo@example.com');

      scan.request.ExpressionAttributeNames.should.eql({'#email' : 'email'});
      scan.request.ExpressionAttributeValues.should.eql({':email' : 'foo@example.com'});
      scan.request.FilterExpression.should.eql('(#email <= :email)');
    });

    it('should have less than clause', function() {
      scan = scan.where('email').lt('foo@example.com');

      scan.request.ExpressionAttributeNames.should.eql({'#email' : 'email'});
      scan.request.ExpressionAttributeValues.should.eql({':email' : 'foo@example.com'});
      scan.request.FilterExpression.should.eql('(#email < :email)');
    });

    it('should have greater than or equal clause', function() {
      scan = scan.where('email').gte('foo@example.com');

      scan.request.ExpressionAttributeNames.should.eql({'#email' : 'email'});
      scan.request.ExpressionAttributeValues.should.eql({':email' : 'foo@example.com'});
      scan.request.FilterExpression.should.eql('(#email >= :email)');
    });

    it('should have greater than clause', function() {
      scan = scan.where('email').gt('foo@example.com');

      scan.request.ExpressionAttributeNames.should.eql({'#email' : 'email'});
      scan.request.ExpressionAttributeValues.should.eql({':email' : 'foo@example.com'});
      scan.request.FilterExpression.should.eql('(#email > :email)');
    });

    it('should have not null clause', function() {
      scan = scan.where('email').notNull();

      scan.request.ExpressionAttributeNames.should.eql({'#email' : 'email'});
      expect(scan.request.ExpressionAttributeValues).to.not.exist;
      scan.request.FilterExpression.should.eql('(attribute_exists(#email))');
    });

    it('should have null clause', function() {
      scan = scan.where('email').null();

      scan.request.ExpressionAttributeNames.should.eql({'#email' : 'email'});
      expect(scan.request.ExpressionAttributeValues).to.not.exist;
      scan.request.FilterExpression.should.eql('(attribute_not_exists(#email))');
    });

    it('should have contains clause', function() {
      scan = scan.where('email').contains('foo@example.com');

      scan.request.ExpressionAttributeNames.should.eql({'#email' : 'email'});
      scan.request.ExpressionAttributeValues.should.eql({':email' : 'foo@example.com'});
      scan.request.FilterExpression.should.eql('(contains(#email, :email))');
    });

    it('should not pass a number set when making contains call', function() {
      scan = scan.where('scores').contains(2);

      scan.request.ExpressionAttributeNames.should.eql({'#scores' : 'scores'});
      scan.request.ExpressionAttributeValues.should.eql({':scores' : 2});
      scan.request.FilterExpression.should.eql('(contains(#scores, :scores))');
    });

    it('should have not contains clause', function() {
      scan = scan.where('email').notContains('foo@example.com');

      scan.request.ExpressionAttributeNames.should.eql({'#email' : 'email'});
      scan.request.ExpressionAttributeValues.should.eql({':email' : 'foo@example.com'});
      scan.request.FilterExpression.should.eql('(NOT contains(#email, :email))');
    });

    it('should have in clause', function() {
      scan = scan.where('email').in(['foo@example.com', 'test@example.com']);

      scan.request.ExpressionAttributeNames.should.eql({'#email' : 'email'});
      scan.request.ExpressionAttributeValues.should.eql({':email' : 'foo@example.com', ':email_2' : 'test@example.com'});
      scan.request.FilterExpression.should.eql('(#email IN (:email,:email_2))');
    });

    it('should have begins with clause', function() {
      scan = scan.where('email').beginsWith('foo');

      scan.request.ExpressionAttributeNames.should.eql({'#email' : 'email'});
      scan.request.ExpressionAttributeValues.should.eql({':email' : 'foo'});
      scan.request.FilterExpression.should.eql('(begins_with(#email, :email))');
    });

    it('should have between clause', function() {
      scan = scan.where('email').between('bob@bob.com', 'foo@foo.com');

      scan.request.ExpressionAttributeNames.should.eql({'#email' : 'email'});
      scan.request.ExpressionAttributeValues.should.eql({':email' : 'bob@bob.com', ':email_2' : 'foo@foo.com'});
      scan.request.FilterExpression.should.eql('(#email BETWEEN :email AND :email_2)');
    });

    it('should have multiple filters', function() {
      scan = scan
        .where('name').equals('Tim')
        .where('email').beginsWith('foo');

      scan.request.ExpressionAttributeNames.should.eql({'#name' : 'name', '#email' : 'email'});
      scan.request.ExpressionAttributeValues.should.eql({':name' : 'Tim', ':email' : 'foo'});
      scan.request.FilterExpression.should.eql('(#name = :name) AND (begins_with(#email, :email))');
    });

    it('should have multiple filters on the same attribute', function() {
      scan = scan
        .where('email').gt('foo@example.com')
        .where('email').lt('moo@example.com');

      scan.request.ExpressionAttributeNames.should.eql({'#email' : 'email'});
      scan.request.ExpressionAttributeValues.should.eql({':email' : 'foo@example.com', ':email_2' : 'moo@example.com'});
      scan.request.FilterExpression.should.eql('(#email > :email) AND (#email < :email_2)');
    });

    it('should convert date to iso string', function() {
      var d = new Date();
      scan = scan.where('created').equals(d);

      scan.request.ExpressionAttributeNames.should.eql({'#created' : 'created'});
      scan.request.ExpressionAttributeValues.should.eql({':created' : d.toISOString()});
      scan.request.FilterExpression.should.eql('(#created = :created)');
    });

  });

  describe('#loadAll', function () {

    it('should set load all option to true', function () {
      var scan = new Scan(table, serializer).loadAll();

      scan.options.loadAll.should.be.true;
    });
  });


  describe('#filterExpression', function () {

    it('should set filter expression', function () {
      var scan = new Scan(table, serializer).filterExpression('Postedby = :val');
      scan.request.FilterExpression.should.equal('Postedby = :val');
    });
  });

  describe('#expressionAttributeValues', function () {

    it('should set expression attribute values', function () {
      var scan = new Scan(table, serializer).expressionAttributeValues({ ':val' : 'test'});
      scan.request.ExpressionAttributeValues.should.eql({ ':val' : 'test'});
    });

  });

  describe('#expressionAttributeNames', function () {

    it('should set expression attribute names', function () {
      var scan = new Scan(table, serializer).expressionAttributeNames({ '#name' : 'name'});
      scan.request.ExpressionAttributeNames.should.eql({ '#name' : 'name'});
    });
  });

  describe('#projectionExpression', function () {

    it('should set projection expression', function () {
      var scan = new Scan(table, serializer).projectionExpression( '#name, #email');
      scan.request.ProjectionExpression.should.eql('#name, #email');
    });
  });

});
