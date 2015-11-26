'use strict';

var helper = require('./test-helper'),
    Schema = require('../lib/schema'),
    Query  = require('../lib//query'),
    Serializer = require('../lib/serializer'),
    Table  = require('../lib/table'),
    chai   = require('chai'),
    expect = chai.expect,
    assert = require('assert'),
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
    table.log = helper.testLogger();
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
      var t = new Table('accounts', s, Serializer, helper.mockDocClient(), helper.testLogger());

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

      var t = new Table('accounts', s, Serializer, helper.mockDocClient(), helper.testLogger());

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
      var config = {
        hashKey: 'name',
        rangeKey: 'email',
        schema : {
          name : Joi.string(),
          email : Joi.string()
        }
      };

      var s = new Schema(config);

      var t = new Table('accounts', s, Serializer, helper.mockDocClient(), helper.testLogger());

      var err = new Error('RetryableException');
      err.retryable = true;

      t.docClient.query
        .onCall(0).yields(err)
        .onCall(1).yields(null, {Items : [ { name : 'Tim Tester', email : 'test@test.com'} ]});

      var stream = new Query('tim', t, Serializer).exec();

      var called = false;

      stream.on('readable', function () {
        called = true;

        var data = stream.read();
        if(data) {
          expect(data.Items).to.have.length.above(0);
        }
      });

      stream.on('end', function () {
        expect(called).to.be.true;
        return done();
      });

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
      query.exec();

      query.request.IndexName.should.equal('UserAgeIndex');

      query.request.ExpressionAttributeNames.should.eql({'#age' : 'age'});
      query.request.ExpressionAttributeValues.should.eql({':age' : 18});
      query.request.KeyConditionExpression.should.eql('(#age = :age)');
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
      var query = new Query('tim', table, serializer).attributes(['created', 'email']);
      query.request.ProjectionExpression.should.eql('#created,#email');
      query.request.ExpressionAttributeNames.should.eql({'#created' : 'created', '#email' : 'email'});
    });

    it('should set single attribute to get', function () {
      var query = new Query('tim', table, serializer).attributes('email');
      query.request.ProjectionExpression.should.eql('#email');
      query.request.ExpressionAttributeNames.should.eql({'#email' : 'email'});
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

    it('should have hash key and range key equals clauses', function() {
      query = query.where('email').equals('foo@example.com');
      query.exec();

      query.request.ExpressionAttributeNames.should.eql({'#email' : 'email', '#name' : 'name'});
      query.request.ExpressionAttributeValues.should.eql({':email' : 'foo@example.com', ':name' : 'tim'});
      query.request.KeyConditionExpression.should.eql('(#email = :email) AND (#name = :name)');
    });

    it('should have equals clause', function() {
      query = query.where('email').equals('foo@example.com');

      query.request.ExpressionAttributeNames.should.eql({'#email' : 'email'});
      query.request.ExpressionAttributeValues.should.eql({':email' : 'foo@example.com'});
      query.request.KeyConditionExpression.should.eql('(#email = :email)');
    });

    it('should have less than or equal clause', function() {
      query = query.where('email').lte('foo@example.com');

      query.request.ExpressionAttributeNames.should.eql({'#email' : 'email'});
      query.request.ExpressionAttributeValues.should.eql({':email' : 'foo@example.com'});
      query.request.KeyConditionExpression.should.eql('(#email <= :email)');
    });

    it('should have less than clause', function() {
      query = query.where('email').lt('foo@example.com');

      query.request.ExpressionAttributeNames.should.eql({'#email' : 'email'});
      query.request.ExpressionAttributeValues.should.eql({':email' : 'foo@example.com'});
      query.request.KeyConditionExpression.should.eql('(#email < :email)');
    });

    it('should have greater than or equal clause', function() {
      query = query.where('email').gte('foo@example.com');

      query.request.ExpressionAttributeNames.should.eql({'#email' : 'email'});
      query.request.ExpressionAttributeValues.should.eql({':email' : 'foo@example.com'});
      query.request.KeyConditionExpression.should.eql('(#email >= :email)');
    });

    it('should have greater than clause', function() {
      query = query.where('email').gt('foo@example.com');

      query.request.ExpressionAttributeNames.should.eql({'#email' : 'email'});
      query.request.ExpressionAttributeValues.should.eql({':email' : 'foo@example.com'});
      query.request.KeyConditionExpression.should.eql('(#email > :email)');
    });

    it('should have begins with clause', function() {
      query = query.where('email').beginsWith('foo');

      query.request.ExpressionAttributeNames.should.eql({'#email' : 'email'});
      query.request.ExpressionAttributeValues.should.eql({':email' : 'foo'});
      query.request.KeyConditionExpression.should.eql('(begins_with(#email, :email))');
    });

    it('should have between clause', function() {
      query = query.where('email').between('bob@bob.com', 'foo@foo.com');

      query.request.ExpressionAttributeNames.should.eql({'#email' : 'email'});
      query.request.ExpressionAttributeValues.should.eql({':email' : 'bob@bob.com', ':email_2' : 'foo@foo.com'});
      query.request.KeyConditionExpression.should.eql('(#email BETWEEN :email AND :email_2)');
    });

    it('should support multiple clauses on same attribute', function() {
      query = query.where('email').gt('foo@example.com').where('email').lt('moo@foo.com');

      query.request.ExpressionAttributeNames.should.eql({'#email' : 'email'});
      query.request.ExpressionAttributeValues.should.eql({':email' : 'foo@example.com', ':email_2' : 'moo@foo.com'});
      query.request.KeyConditionExpression.should.eql('(#email > :email) AND (#email < :email_2)');
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

      query.request.ExpressionAttributeNames.should.eql({'#age' : 'age'});
      query.request.ExpressionAttributeValues.should.eql({':age' : 5});
      query.request.FilterExpression.should.eql('(#age = :age)');
    });

    it('should have exists clause', function() {
      query = query.filter('age').exists();

      query.request.ExpressionAttributeNames.should.eql({'#age' : 'age'});
      expect(query.request.ExpressionAttributeValues).to.not.exist;
      query.request.FilterExpression.should.eql('(attribute_exists(#age))');
    });

    it('should have not exists clause', function() {
      query = query.filter('age').exists(false);

      query.request.ExpressionAttributeNames.should.eql({'#age' : 'age'});
      expect(query.request.ExpressionAttributeValues).to.not.exist;
      query.request.FilterExpression.should.eql('(attribute_not_exists(#age))');
    });

    it('should have between clause', function() {
      query = query.filter('age').between(5, 7);

      query.request.ExpressionAttributeNames.should.eql({'#age' : 'age'});
      query.request.ExpressionAttributeValues.should.eql({':age' : 5, ':age_2' : 7});
      query.request.FilterExpression.should.eql('(#age BETWEEN :age AND :age_2)');
    });

    it('should have IN clause', function() {
      query = query.filter('age').in([5, 7, 12]);

      query.request.ExpressionAttributeNames.should.eql({'#age' : 'age'});
      query.request.ExpressionAttributeValues.should.eql({':age' : 5, ':age_2' : 7, ':age_3' : 12});
      query.request.FilterExpression.should.eql('(#age IN (:age,:age_2,:age_3))');
    });

    it('should support multiple filters on same attribute', function() {
      query = query.filter('age').gt(5).filter('age').lt(20).filter('age').ne(15);

      query.request.ExpressionAttributeNames.should.eql({'#age' : 'age'});
      query.request.ExpressionAttributeValues.should.eql({':age' : 5, ':age_2' : 20, ':age_3' : 15});
      query.request.FilterExpression.should.eql('(#age > :age) AND (#age < :age_2) AND (#age <> :age_3)');
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
