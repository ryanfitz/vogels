'use strict';

var helper = require('./test-helper'),
    Schema = require('../lib/schema'),
    Query  = require('../lib//query'),
    Serializer = require('../lib/serializer'),
    Table  = require('../lib/table'),
    _      = require('lodash'),
    chai   = require('chai'),
    expect = chai.expect,
    assert = require('assert'),
    sinon  = require('sinon'),
    Joi    = require('joi');

chai.should();

describe('Query', function () {
  var serializer,
      table;

  beforeEach(function () {
    serializer = helper.mockSerializer(),

    table = helper.mockTable();
    table.config = {name : 'accounts'};
    table.docClient = helper.mockDocClient();
  });

  describe('#exec', function () {

    it('should run query against table', function (done) {
      var config = {
        hashKey: 'name',
        rangeKey: 'email',
        schema : {
          name : Joi.string(),
          email : Joi.string()
        }
      };

      table.schema = new Schema(config);

      table.runQuery.yields(null, {});
      serializer.serializeItem.returns({name: {S: 'tim'}});

      new Query('tim', table, serializer).exec(function (err, results) {
        results.should.eql({Items: [], Count: 0});
        done();
      });
    });

    it('should return error', function (done) {
      var config = {
        hashKey: 'name',
        rangeKey: 'email',
        schema : {
          name : Joi.string(),
          email : Joi.string()
        }
      };

      var s = new Schema(config);
      var t = new Table('accounts', s, Serializer, helper.mockDocClient());

      t.docClient.query.yields(new Error('Fail'));

      new Query('tim', t, Serializer).exec(function (err, results) {
        expect(err).to.exist;
        expect(results).to.not.exist;
        done();
      });
    });

    it('should stream error', function (done) {
      var config = {
        hashKey: 'name',
        rangeKey: 'email',
        schema : {
          name : Joi.string(),
          email : Joi.string()
        }
      };

      var s = new Schema(config);

      var t = new Table('accounts', s, Serializer, helper.mockDocClient());

      t.docClient.query.yields(new Error('Fail'));

      var stream = new Query('tim', t, Serializer).exec();

      stream.on('error', function (err) {
        expect(err).to.exist;
        return done();
      });

      stream.on('readable', function () {
        assert(false, 'readable should not be called');
      });

    });

    it('should stream data after handling retryable error', function (done) {
      var clock = sinon.useFakeTimers();

      var config = {
        hashKey: 'name',
        rangeKey: 'email',
        schema : {
          name : Joi.string(),
          email : Joi.string()
        }
      };

      var s = new Schema(config);

      var t = new Table('accounts', s, Serializer, helper.mockDocClient());

      var err = new Error('RetryableException');
      err.retryable = true;

      t.docClient.query
        .onCall(0).yields(err)
        .onCall(1).yields(null, {Items : [ { name : 'Tim Tester', email : 'test@test.com'} ]});

      var stream = new Query('tim', t, Serializer).exec();

      var called = false;

     stream.on('readable', function () {
        called = true;
        expect(stream.read().Items).to.have.length.above(0);
      });

      stream.on('end', function () {
        expect(called).to.be.true;

        clock.restore();
        return done();
      });

      clock.tick(2000);
    });
  });

  describe('#limit', function () {
    beforeEach(function () {
      var config = {
        hashKey: 'name',
        rangeKey: 'email',
        schema : {
          name : Joi.string(),
          email : Joi.string()
        }
      };

      table.schema = new Schema(config);
    });

    it('should set the limit', function () {
      var query = new Query('tim', table, serializer).limit(10);
      query.request.Limit.should.equal(10);
    });

    it('should throw when limit is zero', function () {
      var query = new Query('tim', table, serializer);

      expect(function () {
        query.limit(0);
      }).to.throw('Limit must be greater than 0');
    });

  });

  describe('#filterExpression', function () {

    it('should set filter expression', function () {
      var config = {
        hashKey: 'name',
        schema : {
          name : Joi.string(),
        }
      };

      table.schema = new Schema(config);

      var query = new Query('tim', table, serializer).filterExpression('Postedby = :val');

      query.request.FilterExpression.should.equal('Postedby = :val');
    });
  });

  describe('#expressionAttributeValues', function () {

    it('should set expression attribute values', function () {
      var config = {
        hashKey: 'name',
        schema : {
          name : Joi.string(),
        }
      };

      table.schema = new Schema(config);

      var query = new Query('tim', table, serializer).expressionAttributeValues({ ':val' : 'test'});

      query.request.ExpressionAttributeValues.should.eql({ ':val' : 'test'});
    });
  });

  describe('#expressionAttributeNames', function () {

    it('should set expression attribute names', function () {
      var config = {
        hashKey: 'name',
        schema : {
          name : Joi.string(),
        }
      };

      table.schema = new Schema(config);

      var query = new Query('tim', table, serializer).expressionAttributeNames({ '#name' : 'name'});

      query.request.ExpressionAttributeNames.should.eql({ '#name' : 'name'});
    });
  });

  describe('#projectionExpression', function () {

    it('should set projection expression', function () {
      var config = {
        hashKey: 'name',
        schema : {
          name : Joi.string(),
        }
      };

      table.schema = new Schema(config);

      var query = new Query('tim', table, serializer).projectionExpression( '#name, #email');

      query.request.ProjectionExpression.should.eql('#name, #email');
    });
  });

  describe('#usingIndex', function () {

    it('should set the index name to use', function () {
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

      table.schema = new Schema(config);

      var query = new Query('tim', table, serializer).usingIndex('CreatedIndex');

      query.request.IndexName.should.equal('CreatedIndex');
    });

    it('should create key condition for global index hash key', function () {
      var config = {
        hashKey: 'name',
        rangeKey: 'email',
        schema : {
          name : Joi.string(),
          email : Joi.string(),
          age : Joi.number()
        },
        indexes : [{ hashKey : 'age', type : 'global', name : 'UserAgeIndex'}]
      };

      table.schema = new Schema(config);

      serializer.serializeItem.returns({age: {N: '18'}});

      var query = new Query(18, table, serializer).usingIndex('UserAgeIndex');
      query.buildRequest();

      query.request.IndexName.should.equal('UserAgeIndex');
      query.request.KeyConditions.should.have.length(1);

      var cond = _.first(query.request.KeyConditions);
      cond.format().should.eql({AttributeValueList: [{N: '18'}], ComparisonOperator: 'EQ'});
    });
  });

  describe('#consistentRead', function () {
    beforeEach(function () {
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

      table.schema = new Schema(config);
    });

    it('should set Consistent Read to true', function () {
      var query = new Query('tim', table, serializer).consistentRead(true);
      query.request.ConsistentRead.should.be.true;
    });

    it('should set Consistent Read to true when passing no args', function () {
      var query = new Query('tim', table, serializer).consistentRead();
      query.request.ConsistentRead.should.be.true;
    });

    it('should set Consistent Read to false', function () {
      var query = new Query('tim', table, serializer).consistentRead(false);
      query.request.ConsistentRead.should.be.false;
    });

  });

  describe('#attributes', function () {
    beforeEach(function () {
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

      table.schema = new Schema(config);
    });

    it('should set array attributes to get', function () {
      var query = new Query('tim', table, serializer).attributes(['created']);
      query.request.AttributesToGet.should.eql(['created']);
    });

    it('should set single attribute to get', function () {
      var query = new Query('tim', table, serializer).attributes('email');
      query.request.AttributesToGet.should.eql(['email']);
    });

  });

  describe('#order', function () {
    beforeEach(function () {
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

      table.schema = new Schema(config);
    });

    it('should set scan index forward to true', function () {
      var query = new Query('tim', table, serializer).ascending();
      query.request.ScanIndexForward.should.be.true;
    });

    it('should set scan index forward to false', function () {
      var query = new Query('tim', table, serializer).descending();
      query.request.ScanIndexForward.should.be.false;
    });

  });

  describe('#startKey', function () {
    beforeEach(function () {
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

      table.schema = new Schema(config);
    });

    it('should set start Key', function () {
      var key = {name: {S: 'tim'}, email : {S: 'foo@example.com'}};
      serializer.buildKey.returns(key);

      var query = new Query('tim', table, serializer).startKey({name: 'tim', email: 'foo@example.com'});

      query.request.ExclusiveStartKey.should.eql(key);
    });
  });

  describe('#select', function () {

    it('should set select Key', function () {
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

      table.schema = new Schema(config);

      var query = new Query('tim', table, serializer).select('COUNT');

      query.request.Select.should.eql('COUNT');
    });
  });

  describe('#ReturnConsumedCapacity', function () {
    beforeEach(function () {
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

      table.schema = new Schema(config);
    });

    it('should set return consumed capacity Key to passed in value', function () {
      var query = new Query('tim', table, serializer).returnConsumedCapacity('TOTAL');

      query.request.ReturnConsumedCapacity.should.eql('TOTAL');
    });

    it('should set return consumed capacity Key', function () {
      var query = new Query('tim', table, serializer).returnConsumedCapacity();

      query.request.ReturnConsumedCapacity.should.eql('TOTAL');
    });
  });

  describe('#where', function () {
    var query;

    beforeEach(function () {
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

      table.schema = new Schema(config);
      query = new Query('tim', table, serializer);
    });

    it('should have equals clause', function() {
      query = query.where('email').equals('foo@example.com');

      query.request.KeyConditions.should.have.length(1);
      var cond = _.first(query.request.KeyConditions);
      cond.format().should.eql({AttributeValueList: [{S: 'foo@example.com'}], ComparisonOperator: 'EQ'});
    });

    it('should have less than or equal clause', function() {
      query = query.where('email').lte('foo@example.com');

      query.request.KeyConditions.should.have.length(1);
      var cond = _.first(query.request.KeyConditions);
      cond.format().should.eql({AttributeValueList: [{S: 'foo@example.com'}], ComparisonOperator: 'LE'});
    });

    it('should have less than clause', function() {
      query = query.where('email').lt('foo@example.com');

      query.request.KeyConditions.should.have.length(1);
      var cond = _.first(query.request.KeyConditions);
      cond.format().should.eql({AttributeValueList: [{S: 'foo@example.com'}], ComparisonOperator: 'LT'});
    });

    it('should have greater than or equal clause', function() {
      query = query.where('email').gte('foo@example.com');

      query.request.KeyConditions.should.have.length(1);
      var cond = _.first(query.request.KeyConditions);
      cond.format().should.eql({AttributeValueList: [{S: 'foo@example.com'}], ComparisonOperator: 'GE'});
    });

    it('should have greater than clause', function() {
      query = query.where('email').gt('foo@example.com');

      query.request.KeyConditions.should.have.length(1);
      var cond = _.first(query.request.KeyConditions);
      cond.format().should.eql({AttributeValueList: [{S: 'foo@example.com'}], ComparisonOperator: 'GT'});
    });

    it('should have begins with clause', function() {
      query = query.where('email').beginsWith('foo');

      query.request.KeyConditions.should.have.length(1);
      var cond = _.first(query.request.KeyConditions);
      cond.format().should.eql({AttributeValueList: [{S: 'foo'}], ComparisonOperator: 'BEGINS_WITH'});
    });

    it('should have between clause', function() {
      query = query.where('email').between('bob@bob.com', 'foo@foo.com');

      var expect = {
        AttributeValueList: [
          {S: 'bob@bob.com'},
          {S: 'foo@foo.com'}
        ],
        ComparisonOperator: 'BETWEEN'
      };

      //query.request.KeyConditions.email.should.eql(expect);

      query.request.KeyConditions.should.have.length(1);
      var cond = _.first(query.request.KeyConditions);
      cond.format().should.eql(expect);
    });

  });

  describe('#filter', function () {
    var query;

    beforeEach(function () {
      var config = {
        hashKey: 'name',
        rangeKey: 'email',
        schema : {
          name : Joi.string(),
          email : Joi.string(),
          created : Joi.date(),
          age : Joi.number()
        },
        indexes : [{ hashKey : 'name', rangeKey : 'created', type : 'local', name : 'CreatedIndex'}]
      };

      table.schema = new Schema(config);

      query = new Query('tim', table, serializer);
    });

    it('should have equals clause', function() {
      query = query.filter('age').equals(5);

      query.request.QueryFilter.should.have.length(1);
      var cond = _.first(query.request.QueryFilter);
      cond.format().should.eql({AttributeValueList: [{N: '5'}], ComparisonOperator: 'EQ'});
    });

    it('should have exists clause', function() {
      query = query.filter('age').exists();

      query.request.QueryFilter.should.have.length(1);
      var cond = _.first(query.request.QueryFilter);
      cond.format().should.eql({ComparisonOperator: 'NOT_NULL'});
    });

    it('should have not exists clause', function() {
      query = query.filter('age').exists(false);

      query.request.QueryFilter.should.have.length(1);
      var cond = _.first(query.request.QueryFilter);
      cond.format().should.eql({ComparisonOperator: 'NULL'});
    });

    it('should have between clause', function() {
      query = query.filter('age').between(5, 7);

      var expected = {
        AttributeValueList: [
          {N: '5'},
          {N: '7'}
        ],
        ComparisonOperator: 'BETWEEN'
      };

      query.request.QueryFilter.should.have.length(1);
      var cond = _.first(query.request.QueryFilter);
      cond.format().should.eql(expected);
    });

    it.skip('should have IN clause', function() {
      // TODO ther is a bug in the dynamodb-doc lib
      // that needs to get fixed before this test can pass
      query = query.filter('age').in([5, 7]);

      var expected = {
        AttributeValueList: [
          {N: '5'},
          {N: '7'}
        ],
        ComparisonOperator: 'IN'
      };

      query.request.QueryFilter.should.have.length(1);
      var cond = _.first(query.request.QueryFilter);
      cond.format().should.eql(expected);
    });

  });

  describe('#loadAll', function () {

    it('should set load all option to true', function () {
      var config = {
        hashKey: 'name',
        rangeKey: 'email',
        schema : {
          name : Joi.string(),
          email : Joi.string(),
        }
      };

      table.schema = new Schema(config);

      var query = new Query('tim', table, serializer).loadAll();

      query.options.loadAll.should.be.true;
    });
  });

});
