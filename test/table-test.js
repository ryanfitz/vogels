'use strict';

var helper = require('./test-helper'),
    _      = require('lodash'),
    Joi    = require('joi'),
    Table  = require('../lib/table'),
    Schema = require('../lib/schema'),
    Query  = require('../lib//query'),
    Scan   = require('../lib//scan'),
    Item   = require('../lib/item'),
    realSerializer = require('../lib/serializer'),
    chai   = require('chai'),
    expect = chai.expect,
    sinon  = require('sinon');

chai.should();

describe('table', function () {
  var table,
      serializer,
      docClient,
      dynamodb,
      logger;

  beforeEach(function () {
    serializer = helper.mockSerializer(),
    docClient = helper.mockDocClient();
    dynamodb = docClient.service;
    logger = helper.testLogger();
  });

  describe('#get', function () {

    it('should get item by hash key', function (done) {
      var config = {
        hashKey: 'email'
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      var request = {
        TableName: 'accounts',
        Key : { email : 'test@test.com'}
      };

      var resp = {
        Item : {email: 'test@test.com', name: 'test dude'}
      };

      docClient.get.withArgs(request).yields(null, resp);

      table.get('test@test.com', function (err, account) {
        account.should.be.instanceof(Item);
        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('test dude');

        done();
      });
    });

    it('should get item by hash and range key', function (done) {
      var config = {
        hashKey: 'name',
        rangeKey: 'email'
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      var request = {
        TableName: 'accounts',
        Key : {
          name  : 'Tim Tester',
          email : 'test@test.com'
        }
      };

      var resp = {
        Item : {email: 'test@test.com', name: 'Tim Tester'}
      };

      docClient.get.withArgs(request).yields(null, resp);

      table.get('Tim Tester', 'test@test.com', function (err, account) {
        account.should.be.instanceof(Item);
        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('Tim Tester');

        done();
      });
    });

    it('should get item by hash key and options', function (done) {
      var config = {
        hashKey: 'email',
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      var request = {
        TableName: 'accounts',
        Key : { email : 'test@test.com' },
        ConsistentRead: true
      };

      var resp = {
        Item : {email: 'test@test.com', name: 'test dude'}
      };

      docClient.get.withArgs(request).yields(null, resp);

      table.get('test@test.com', {ConsistentRead: true}, function (err, account) {
        account.should.be.instanceof(Item);
        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('test dude');

        done();
      });
    });

    it('should get item by hashkey, range key and options', function (done) {
      var config = {
        hashKey: 'name',
        rangeKey: 'email',
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      var request = {
        TableName: 'accounts',
        Key : {
          name  : 'Tim Tester',
          email : 'test@test.com'
        },
        ConsistentRead: true
      };

      var resp = {
        Item : {email: 'test@test.com', name: 'Tim Tester'}
      };

      docClient.get.withArgs(request).yields(null, resp);

      table.get('Tim Tester', 'test@test.com', {ConsistentRead: true}, function (err, account) {
        account.should.be.instanceof(Item);
        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('Tim Tester');

        done();
      });
    });

    it('should get item from dynamic table by hash key', function (done) {

      var config = {
        hashKey: 'email',
        tableName : function () {
          return 'accounts_2014';
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      var request = {
        TableName: 'accounts_2014',
        Key : { email : 'test@test.com' }
      };

      var resp = {
        Item : {email: 'test@test.com', name: 'test dude'}
      };

      docClient.get.withArgs(request).yields(null, resp);

      table.get('test@test.com', function (err, account) {
        account.should.be.instanceof(Item);
        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('test dude');

        done();
      });
    });

    it('should return error', function (done) {
      var config = {
        hashKey: 'email',
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      docClient.get.yields(new Error('Fail'));

      table.get('test@test.com', function (err, account) {
        expect(err).to.exist;
        expect(account).to.not.exist;
        done();
      });
    });

  });

  describe('#create', function () {

    it('should create valid item', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email : Joi.string(),
          name  : Joi.string(),
          age   : Joi.number()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      var request = {
        TableName: 'accounts',
        Item : {
          email : 'test@test.com',
          name  : 'Tim Test',
          age   : 23
        }
      };

      docClient.put.withArgs(request).yields(null, {});

      table.create(request.Item, function (err, account) {
        expect(err).to.not.exist;
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('Tim Test');

        done();
      });
    });

    it('should call apply defaults', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email : Joi.string(),
          name  : Joi.string().default('Foo'),
          age   : Joi.number()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      var request = {
        TableName: 'accounts',
        Item : {
          email : 'test@test.com',
          name  : 'Foo',
          age   : 23
        }
      };

      docClient.put.withArgs(request).yields(null, {});

      table.create({email : 'test@test.com', age: 23}, function (err, account) {
        expect(err).to.not.exist;
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('Foo');

        done();
      });
    });

    it('should omit null values', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email : Joi.string(),
          name  : Joi.string(),
          age   : Joi.number().allow(null),
          favoriteNumbers : Schema.types.numberSet().allow(null),
          luckyNumbers : Schema.types.numberSet().allow(null)
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      var numberSet = sinon.match(function (value) {
        var s = docClient.createSet([1, 2, 3]);

        value.type.should.eql('Number');
        value.values.should.eql(s.values);

        return true;
      }, 'NumberSet');

      var request = {
        TableName: 'accounts',
        Item : {
          email : 'test@test.com',
          name  : 'Tim Test',
          luckyNumbers: numberSet
        }
      };

      docClient.put.withArgs(request).yields(null, {});

      var item = {email : 'test@test.com', name : 'Tim Test', age : null, favoriteNumbers: [], luckyNumbers: [1, 2, 3]};
      table.create(item, function (err, account) {
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('Tim Test');
        account.get('luckyNumbers').should.eql([1, 2, 3]);

        expect(account.toJSON()).to.have.keys(['email', 'name', 'luckyNumbers']);

        done();
      });
    });

    it('should omit empty values', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email : Joi.string(),
          name  : Joi.string().allow(''),
          age   : Joi.number()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      var request = {
        TableName: 'accounts',
        Item : {
          email : 'test@test.com',
          age   : 2
        }
      };

      docClient.put.withArgs(request).yields(null, {});

      table.create({email: 'test@test.com', name: '', age: 2}, function (err, account) {
        expect(err).to.not.exist;
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        account.get('age').should.equal(2);

        done();
      });
    });

    it('should create item with createdAt timestamp', function (done) {
      var config = {
        hashKey: 'email',
        timestamps : true,
        schema : {
          email : Joi.string(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      var request = {
        TableName: 'accounts',
        Item : {
          email : 'test@test.com',
          createdAt : sinon.match.string
        }
      };

      docClient.put.withArgs(request).yields(null, {});

      table.create({email : 'test@test.com'}, function (err, account) {
        expect(err).to.not.exist;
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        account.get('createdAt').should.exist;
        done();
      });
    });

    it('should create item with custom createdAt attribute name', function (done) {
      var config = {
        hashKey: 'email',
        timestamps : true,
        createdAt : 'created',
        schema : {
          email : Joi.string(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      var request = {
        TableName: 'accounts',
        Item : {
          email : 'test@test.com',
          created : sinon.match.string
        }
      };

      docClient.put.withArgs(request).yields(null, {});

      table.create({email : 'test@test.com'}, function (err, account) {
        expect(err).to.not.exist;
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        account.get('created').should.exist;
        done();
      });
    });


    it('should create item without createdAt param', function (done) {
      var config = {
        hashKey: 'email',
        timestamps : true,
        createdAt : false,
        schema : {
          email : Joi.string(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      var request = {
        TableName: 'accounts',
        Item : {
          email : 'test@test.com'
        }
      };

      docClient.put.withArgs(request).yields(null, {});

      table.create({email : 'test@test.com'}, function (err, account) {
        expect(err).to.not.exist;
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        expect(account.get('createdAt')).to.not.exist;
        done();
      });
    });

    it('should create item with expected option', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email : Joi.string(),
          name : Joi.string()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      var request = {
        TableName: 'accounts',
        Item : {
          email : 'test@test.com',
        },
        ExpressionAttributeNames: { '#name': 'name' },
        ExpressionAttributeValues: { ':name': 'Foo Bar' },
        ConditionExpression : '(#name = :name)'
      };

      docClient.put.withArgs(request).yields(null, {});

      table.create({email : 'test@test.com'}, {expected: {name: 'Foo Bar'}}, function (err, account) {
        expect(err).to.not.exist;
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        done();
      });
    });

    it('should create item with no callback', function (done) {
      var config = {
        hashKey: 'email',
        timestamps : true,
        schema : {
          email : Joi.string(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      var request = {
        TableName: 'accounts',
        Item : {
          email : 'test@test.com',
        }
      };

      docClient.put.withArgs(request).yields(null, {});

      table.create({email : 'test@test.com'});

      docClient.put.calledWith(request);
      return done();
    });

    it('should return validation error', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email : Joi.string(),
          name : Joi.string()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      table.create({email : 'test@test.com', name : [1, 2, 3]}, function (err, account) {
        expect(err).to.exist;
        expect(err).to.match(/ValidationError/);
        expect(account).to.not.exist;

        sinon.assert.notCalled(docClient.put);
        done();
      });
    });

    it('should create item with condition expression on hashkey when overwrite flag is false', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email : Joi.string(),
          name : Joi.string()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      var request = {
        TableName: 'accounts',
        Item : {
          email : 'test@test.com',
          name : 'Bob Tester'
        },
        ExpressionAttributeNames: { '#email': 'email' },
        ExpressionAttributeValues: { ':email': 'test@test.com' },
        ConditionExpression : '(#email <> :email)'
      };

      docClient.put.withArgs(request).yields(null, {});

      table.create({email : 'test@test.com', name : 'Bob Tester'}, {overwrite: false}, function (err, account) {
        expect(err).to.not.exist;
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        done();
      });
    });

    it('should create item with condition expression on hash and range key when overwrite flag is false', function (done) {
      var config = {
        hashKey: 'email',
        rangeKey: 'name',
        schema : {
          email : Joi.string(),
          name : Joi.string()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      var request = {
        TableName: 'accounts',
        Item : {
          email : 'test@test.com',
          name : 'Bob Tester'
        },
        ExpressionAttributeNames: { '#email': 'email', '#name' : 'name' },
        ExpressionAttributeValues: { ':email': 'test@test.com', ':name' : 'Bob Tester' },
        ConditionExpression : '(#email <> :email) AND (#name <> :name)'
      };

      docClient.put.withArgs(request).yields(null, {});

      table.create({email : 'test@test.com', name : 'Bob Tester'}, {overwrite: false}, function (err, account) {
        expect(err).to.not.exist;
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        done();
      });
    });

    it('should create item without condition expression when overwrite flag is true', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email : Joi.string(),
          name : Joi.string()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      var request = {
        TableName: 'accounts',
        Item : {
          email : 'test@test.com',
          name : 'Bob Tester'
        }
      };

      docClient.put.withArgs(request).yields(null, {});

      table.create({email : 'test@test.com', name : 'Bob Tester'}, {overwrite: true}, function (err, account) {
        expect(err).to.not.exist;
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        done();
      });
    });

  });

  describe('#update', function () {

    it('should update valid item', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email : Joi.string(),
          name  : Joi.string(),
          age   : Joi.number(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      var request = {
        TableName: 'accounts',
        Key : { email : 'test@test.com'},
        ReturnValues: 'ALL_NEW',
        UpdateExpression : 'SET #name = :name, #age = :age',
        ExpressionAttributeValues : { ':name' : 'Tim Test', ':age' : 23},
        ExpressionAttributeNames : { '#name' : 'name', '#age' : 'age'}
      };

      var returnedAttributes = {
        email  : 'test@test.com',
        name   : 'Tim Test',
        age    : 23,
        scores : [97, 86]
      };

      docClient.update.withArgs(request).yields(null, {Attributes: returnedAttributes});

      var item = {email : 'test@test.com', name : 'Tim Test', age : 23};
      table.update(item, function (err, account) {
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('Tim Test');
        account.get('age').should.equal(23);
        account.get('scores').should.eql([97, 86]);

        done();
      });
    });

    it('should update with passed in options', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email : Joi.string(),
          name  : Joi.string(),
          age   : Joi.number(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      var request = {
        TableName: 'accounts',
        Key : { email : 'test@test.com' },
        ReturnValues: 'ALL_OLD',
        UpdateExpression : 'SET #name = :name, #age = :age',
        ExpressionAttributeValues : { ':name_2' : 'Foo Bar', ':name' : 'Tim Test', ':age' : 23 },
        ExpressionAttributeNames : { '#name' : 'name', '#age' : 'age'},
        ConditionExpression : '(#name = :name_2)'
      };

      var returnedAttributes = {
        email  : 'test@test.com',
        name   : 'Tim Test',
        age    : 23,
        scores : [97, 86]
      };

      var item = {email : 'test@test.com', name : 'Tim Test', age : 23};

      docClient.update.withArgs(request).yields(null, {Attributes: returnedAttributes});

      table.update(item, {ReturnValues: 'ALL_OLD', expected: {name: 'Foo Bar'}}, function (err, account) {
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('Tim Test');
        account.get('age').should.equal(23);
        account.get('scores').should.eql([97, 86]);

        done();
      });
    });

    it('should update merge update expressions when passed in as options', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email : Joi.string(),
          name  : Joi.string(),
          age   : Joi.number(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      var request = {
        TableName: 'accounts',
        Key : { email : 'test@test.com' },
        ReturnValues: 'ALL_NEW',
        UpdateExpression : 'SET #name = :name, #age = :age ADD #color :c',
        ExpressionAttributeValues : { ':name' : 'Tim Test', ':age' : 23, ':c' : 'red'},
        ExpressionAttributeNames : { '#name' : 'name', '#age' : 'age', '#color' : 'color'}
      };

      var returnedAttributes = {
        email  : 'test@test.com',
        name   : 'Tim Test',
        age    : 23,
        scores : [97, 86],
        color  : 'red'
      };

      var item = {email : 'test@test.com', name : 'Tim Test', age : 23};

      docClient.update.withArgs(request).yields(null, {Attributes: returnedAttributes});

      var options = {
        UpdateExpression : 'ADD #color :c',
        ExpressionAttributeValues : { ':c' : 'red'},
        ExpressionAttributeNames : { '#color' : 'color'}
      };

      table.update(item, options, function (err, account) {
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('Tim Test');
        account.get('age').should.equal(23);
        account.get('scores').should.eql([97, 86]);
        account.get('color').should.eql('red');

        done();
      });
    });

    it('should update valid item without a callback', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email : Joi.string(),
          name  : Joi.string(),
          age   : Joi.number(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      var request = {
        TableName: 'accounts',
        Key : { email : 'test@test.com'},
        ReturnValues: 'ALL_NEW',
        UpdateExpression : 'SET #name = :name, #age = :age',
        ExpressionAttributeValues : { ':name' : 'Tim Test', ':age' : 23},
        ExpressionAttributeNames : { '#name' : 'name', '#age' : 'age'}
      };

      var returnedAttributes = {
        email  : 'test@test.com',
        name   : 'Tim Test',
        age    : 23,
        scores : [97, 86]
      };

      docClient.update.withArgs(request).yields(null, {Attributes: returnedAttributes});

      var item = {email : 'test@test.com', name : 'Tim Test', age : 23};
      table.update(item);

      docClient.update.calledWith(request);
      return done();
    });

    it('should return error', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email : Joi.string(),
          name  : Joi.string(),
          age   : Joi.number(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      docClient.update.yields(new Error('Fail'));

      var item = {email : 'test@test.com', name : 'Tim Test', age : 23};

      table.update(item, function (err, account) {
        expect(err).to.exist;
        expect(account).to.not.exist;
        done();
      });
    });

  });

  describe('#query', function () {

    it('should return query object', function () {
      var config = {
        hashKey: 'name',
        rangeKey: 'email',
        schema : {
          name  : Joi.string(),
          email : Joi.string()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      table.query('Bob').should.be.instanceof(Query);
    });
  });

  describe('#scan', function () {

    it('should return scan object', function () {
      var config = {
        hashKey: 'name',
        rangeKey: 'email',
        schema : {
          name  : Joi.string(),
          email : Joi.string()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      table.scan().should.be.instanceof(Scan);
    });
  });

  describe('#destroy', function () {

    it('should destroy valid item', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          name  : Joi.string(),
          email : Joi.string(),
          age : Joi.number()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      var request = {
        TableName: 'accounts',
        Key : {
          email : 'test@test.com'
        }
      };

      docClient.delete.yields(null, {});

      serializer.buildKey.returns(request.Key);

      table.destroy('test@test.com', function () {
        serializer.buildKey.calledWith('test@test.com', null, s).should.be.true;
        docClient.delete.calledWith(request).should.be.true;

        done();
      });
    });

    it('should take optional params', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          name  : Joi.string(),
          email : Joi.string(),
          age : Joi.number()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      var request = {
        TableName: 'accounts',
        Key : {
          email : {S : 'test@test.com'}
        },
        ReturnValues : 'ALL_OLD'
      };

      docClient.delete.yields(null, {});

      serializer.buildKey.returns(request.Key);

      table.destroy('test@test.com', {ReturnValues: 'ALL_OLD'}, function () {
        serializer.buildKey.calledWith('test@test.com', null, s).should.be.true;
        docClient.delete.calledWith(request).should.be.true;

        done();
      });
    });

    it('should parse and return attributes', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          name  : Joi.string(),
          email : Joi.string(),
          age : Joi.number()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      var request = {
        TableName: 'accounts',
        Key : { email : 'test@test.com' },
        ReturnValues : 'ALL_OLD'
      };

      var returnedAttributes = {
        email : 'test@test.com',
        name  : 'Foo Bar'
      };

      docClient.delete.yields(null, {Attributes: returnedAttributes});

      serializer.buildKey.returns(request.Key);
      serializer.deserializeItem.withArgs(returnedAttributes).returns(
        {email : 'test@test.com', name: 'Foo Bar'
      });

      table.destroy('test@test.com', {ReturnValues: 'ALL_OLD'}, function (err, item) {
        serializer.buildKey.calledWith('test@test.com', null, s).should.be.true;
        docClient.delete.calledWith(request).should.be.true;

        item.get('name').should.equal('Foo Bar');

        done();
      });
    });

    it('should accept hash and range key', function (done) {
      var config = {
        hashKey: 'email',
        rangeKey: 'name',
        schema : {
          name  : Joi.string(),
          email : Joi.string(),
          age : Joi.number()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      var request = {
        TableName: 'accounts',
        Key : {
          email : 'test@test.com',
          name : 'Foo Bar'
        }
      };

      var returnedAttributes = {
        email : 'test@test.com',
        name  : 'Foo Bar'
      };

      docClient.delete.yields(null, {Attributes: returnedAttributes});

      serializer.buildKey.returns(request.Key);
      serializer.deserializeItem.withArgs(returnedAttributes).returns(
        {email : 'test@test.com', name: 'Foo Bar'
      });

      table.destroy('test@test.com', 'Foo Bar', function (err, item) {
        serializer.buildKey.calledWith('test@test.com', 'Foo Bar', s).should.be.true;
        docClient.delete.calledWith(request).should.be.true;

        item.get('name').should.equal('Foo Bar');

        done();
      });
    });

    it('should accept hashkey rangekey and options', function (done) {
      var config = {
        hashKey: 'email',
        rangeKey: 'name',
        schema : {
          name  : Joi.string(),
          email : Joi.string(),
          age : Joi.number()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      var request = {
        TableName: 'accounts',
        Key : {
          email : 'test@test.com',
          name  : 'Foo Bar'
        },
        ReturnValues : 'ALL_OLD'
      };

      var returnedAttributes = {
        email : 'test@test.com',
        name  : 'Foo Bar'
      };

      docClient.delete.yields(null, {Attributes: returnedAttributes});

      serializer.buildKey.returns(request.Key);
      serializer.deserializeItem.withArgs(returnedAttributes).returns(
        {email : 'test@test.com', name: 'Foo Bar'
      });

      table.destroy('test@test.com', 'Foo Bar', {ReturnValues: 'ALL_OLD'}, function (err, item) {
        serializer.buildKey.calledWith('test@test.com', 'Foo Bar', s).should.be.true;
        docClient.delete.calledWith(request).should.be.true;

        item.get('name').should.equal('Foo Bar');

        done();
      });
    });

    it('should serialize expected option', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          name  : Joi.string(),
          email : Joi.string(),
          age : Joi.number()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      var request = {
        TableName: 'accounts',
        Key : {
          email : 'test@test.com'
        },
        ExpressionAttributeNames: { '#name': 'name' },
        ExpressionAttributeValues: { ':name': 'Foo Bar' },
        ConditionExpression : '(#name = :name)'
      };

      docClient.delete.yields(null, {});

      serializer.serializeItem.withArgs(s, {name: 'Foo Bar'}, {expected : true}).returns(request.Expected);
      serializer.buildKey.returns(request.Key);

      table.destroy('test@test.com', {expected: {name : 'Foo Bar'}}, function () {
        serializer.buildKey.calledWith('test@test.com', null, s).should.be.true;
        docClient.delete.calledWith(request).should.be.true;

        done();
      });
    });

    it('should call delete item without callback', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          name  : Joi.string(),
          email : Joi.string(),
          age : Joi.number()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      var request = {
        TableName: 'accounts',
        Key : {
          email : 'test@test.com'
        }
      };

      docClient.delete.yields(null, {});
      table.destroy('test@test.com');

      docClient.delete.calledWith(request);

      return done();
    });

    it('should call delete item with hash key, options and no callback', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          name  : Joi.string(),
          email : Joi.string(),
          age : Joi.number()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      var request = {
        TableName: 'accounts',
        Key : {
          email : 'test@test.com'
        },
        Expected : {
          name : {'Value' : 'Foo Bar'}
        }
      };

      docClient.delete.yields(null, {});
      table.destroy('test@test.com', {expected: {name : 'Foo Bar'}});

      docClient.delete.calledWith(request);

      return done();
    });
  });

  describe('#createTable', function () {
    it('should create table with hash key', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          name  : Joi.string(),
          email : Joi.string(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      var request = {
        TableName: 'accounts',
        AttributeDefinitions : [
          { AttributeName: 'email', AttributeType: 'S' }
        ],
        KeySchema: [
          { AttributeName: 'email', KeyType: 'HASH' }
        ],
        ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 }
      };

      dynamodb.createTable.yields(null, {});

      table.createTable({readCapacity : 5, writeCapacity: 5}, function (err) {
        expect(err).to.be.null;
        dynamodb.createTable.calledWith(request).should.be.true;
        done();
      });

    });

    it('should create table with range key', function (done) {
      var config = {
        hashKey: 'name',
        rangeKey: 'email',
        schema : {
          name  : Joi.string(),
          email : Joi.string(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      var request = {
        TableName: 'accounts',
        AttributeDefinitions : [
          { AttributeName: 'name', AttributeType: 'S' },
          { AttributeName: 'email', AttributeType: 'S' }
        ],
        KeySchema: [
          { AttributeName: 'name', KeyType: 'HASH' },
          { AttributeName: 'email', KeyType: 'RANGE' }
        ],
        ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 }
      };

      dynamodb.createTable.yields(null, {});

      table.createTable({readCapacity : 5, writeCapacity: 5}, function (err) {
        expect(err).to.be.null;
        dynamodb.createTable.calledWith(request).should.be.true;
        done();
      });

    });

    it('should create table with secondary index', function (done) {
      var config = {
        hashKey: 'name',
        rangeKey: 'email',
        indexes : [
          { hashKey : 'name', rangeKey : 'age', name : 'ageIndex', type : 'local' }
        ],
        schema : {
          name  : Joi.string(),
          email : Joi.string(),
          age   : Joi.number()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      var request = {
        TableName: 'accounts',
        AttributeDefinitions : [
          { AttributeName: 'name', AttributeType: 'S' },
          { AttributeName: 'email', AttributeType: 'S' },
          { AttributeName: 'age', AttributeType: 'N' }
        ],
        KeySchema: [
          { AttributeName: 'name', KeyType: 'HASH' },
          { AttributeName: 'email', KeyType: 'RANGE' }
        ],
        LocalSecondaryIndexes : [
          {
            IndexName : 'ageIndex',
            KeySchema: [
              { AttributeName: 'name', KeyType: 'HASH' },
              { AttributeName: 'age', KeyType: 'RANGE' }
            ],
            Projection : {
              ProjectionType : 'ALL'
            }
          }
        ],
        ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 }
      };

      dynamodb.createTable.yields(null, {});

      table.createTable({readCapacity : 5, writeCapacity: 5}, function (err) {
        expect(err).to.be.null;
        dynamodb.createTable.calledWith(request).should.be.true;
        done();
      });
    });

    it('should create table with global secondary index', function (done) {
      var config = {
        hashKey: 'userId',
        rangeKey: 'gameTitle',
        indexes : [
          { hashKey : 'gameTitle', rangeKey : 'topScore', name : 'GameTitleIndex', type : 'global' }
        ],
        schema : {
          userId  : Joi.string(),
          gameTitle : Joi.string(),
          topScore  : Joi.number()
        }
      };

      var s = new Schema(config);

      table = new Table('gameScores', s, serializer, docClient, logger);

      var request = {
        TableName: 'gameScores',
        AttributeDefinitions : [
          { AttributeName: 'userId', AttributeType: 'S' },
          { AttributeName: 'gameTitle', AttributeType: 'S' },
          { AttributeName: 'topScore', AttributeType: 'N' }
        ],
        KeySchema: [
          { AttributeName: 'userId', KeyType: 'HASH' },
          { AttributeName: 'gameTitle', KeyType: 'RANGE' }
        ],
        GlobalSecondaryIndexes : [
          {
            IndexName : 'GameTitleIndex',
            KeySchema: [
              { AttributeName: 'gameTitle', KeyType: 'HASH' },
              { AttributeName: 'topScore', KeyType: 'RANGE' }
            ],
            Projection : {
              ProjectionType : 'ALL'
            },
            ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 }
          }
        ],
        ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 }
      };

      dynamodb.createTable.yields(null, {});

      table.createTable({readCapacity : 5, writeCapacity: 5}, function (err) {
        expect(err).to.be.null;
        dynamodb.createTable.calledWith(request).should.be.true;
        done();
      });
    });

    it('should create table with global secondary index', function (done) {
      var config = {
        hashKey: 'userId',
        rangeKey: 'gameTitle',
        indexes : [{
          hashKey : 'gameTitle',
          rangeKey : 'topScore',
          name : 'GameTitleIndex',
          type : 'global',
          readCapacity : 10,
          writeCapacity : 5,
          projection: { NonKeyAttributes: [ 'wins' ], ProjectionType: 'INCLUDE' }
        }],
        schema : {
          userId  : Joi.string(),
          gameTitle : Joi.string(),
          topScore  : Joi.number()
        }
      };

      var s = new Schema(config);

      table = new Table('gameScores', s, serializer, docClient, logger);

      var request = {
        TableName: 'gameScores',
        AttributeDefinitions : [
          { AttributeName: 'userId', AttributeType: 'S' },
          { AttributeName: 'gameTitle', AttributeType: 'S' },
          { AttributeName: 'topScore', AttributeType: 'N' }
        ],
        KeySchema: [
          { AttributeName: 'userId', KeyType: 'HASH' },
          { AttributeName: 'gameTitle', KeyType: 'RANGE' }
        ],
        GlobalSecondaryIndexes : [
          {
            IndexName : 'GameTitleIndex',
            KeySchema: [
              { AttributeName: 'gameTitle', KeyType: 'HASH' },
              { AttributeName: 'topScore', KeyType: 'RANGE' }
            ],
            Projection: {
              NonKeyAttributes: [ 'wins' ],
              ProjectionType: 'INCLUDE'
            },
            ProvisionedThroughput: { ReadCapacityUnits: 10, WriteCapacityUnits: 5 }
          }
        ],
        ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 }
      };

      dynamodb.createTable.yields(null, {});

      table.createTable({readCapacity : 5, writeCapacity: 5}, function (err) {
        expect(err).to.be.null;
        dynamodb.createTable.calledWith(request).should.be.true;
        done();
      });
    });
  });

  describe('#describeTable', function () {

    it('should make describe table request', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email  : Joi.string(),
          name : Joi.string(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      var request = {
        TableName: 'accounts'
      };

      dynamodb.describeTable.yields(null, {});

      table.describeTable(function (err) {
        expect(err).to.be.null;
        dynamodb.describeTable.calledWith(request).should.be.true;
        done();
      });
    });

  });

  describe('#updateTable', function () {

    beforeEach(function () {
      var config = {
        hashKey: 'email',
        schema : {
          email  : Joi.string(),
          name : Joi.string(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);
    });

    it('should make update table request', function (done) {
      var request = {
        TableName: 'accounts',
        ProvisionedThroughput: { ReadCapacityUnits: 4, WriteCapacityUnits: 2 }
      };

      dynamodb.describeTable.yields(null, {});
      dynamodb.updateTable.yields(null, {});

      table.updateTable({readCapacity: 4, writeCapacity: 2}, function (err) {
        expect(err).to.be.null;
        dynamodb.updateTable.calledWith(request).should.be.true;
        done();
      });
    });

    it('should make update table request without callback', function (done) {
      var request = {
        TableName: 'accounts',
        ProvisionedThroughput: { ReadCapacityUnits: 2, WriteCapacityUnits: 1 }
      };

      table.updateTable({readCapacity: 2, writeCapacity: 1});

      dynamodb.updateTable.calledWith(request).should.be.true;

      return done();
    });
  });

  describe('#deleteTable', function () {

    beforeEach(function () {
      var config = {
        hashKey: 'email',
        schema : {
          email  : Joi.string(),
          name : Joi.string(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);
    });

    it('should make delete table request', function (done) {
      var request = {
        TableName: 'accounts'
      };

      dynamodb.deleteTable.yields(null, {});

      table.deleteTable(function (err) {
        expect(err).to.be.null;
        dynamodb.deleteTable.calledWith(request).should.be.true;
        done();
      });
    });

    it('should make delete table request without callback', function (done) {
      var request = {
        TableName: 'accounts',
      };

      table.deleteTable();

      dynamodb.deleteTable.calledWith(request).should.be.true;

      return done();
    });
  });

  describe('#tableName', function () {

    it('should return given name', function () {
      var config = {
        hashKey: 'email',
        schema : {
          email  : Joi.string(),
          name : Joi.string(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      table.tableName().should.eql('accounts');
    });

    it('should return table name set on schema', function () {
      var config = {
        hashKey: 'email',
        tableName : 'accounts-2014-03',
        schema : {
          email  : Joi.string(),
          name : Joi.string(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      table.tableName().should.eql('accounts-2014-03');
    });

    it('should return table name returned from function on schema', function () {
      var d = new Date();
      var dateString = [d.getFullYear(), d.getMonth() + 1].join('_');

      var nameFunc = function () {
        return 'accounts_' + dateString;
      };

      var config = {
        hashKey: 'email',
        tableName : nameFunc,
        schema : {
          email  : Joi.string(),
          name : Joi.string(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      table.tableName().should.eql('accounts_' + dateString);
    });

  });

  describe('hooks', function () {

    describe('#create', function () {

      it('should call before hooks', function (done) {

        var config = {
          hashKey: 'email',
          schema : {
            email  : Joi.string(),
            name : Joi.string(),
            age : Joi.number()
          }
        };

        var s = new Schema(config);

        table = new Table('accounts', s, serializer, docClient, logger);

        var item = {email : 'test@test.com', name : 'Tim Test', age : 23};
        docClient.put.yields(null, {});

        serializer.serializeItem.withArgs(s, {email : 'test@test.com', name : 'Tommy', age : 23}).returns({});

        table.before('create', function (data, next) {
          expect(data).to.exist;
          data.name = 'Tommy';

          return next(null, data);
        });

        table.before('create', function (data, next) {
          expect(data).to.exist;
          data.age = '25';

          return next(null, data);
        });

        table.create(item, function (err, item) {
          expect(err).to.not.exist;
          item.get('name').should.equal('Tommy');
          item.get('age').should.equal('25');

          return done();
        });
      });

      it('should return error when before hook returns error', function (done) {
        var config = {
          hashKey: 'email',
          schema : {
            email  : Joi.string(),
            name : Joi.string(),
            age : Joi.number()
          }
        };

        var s = new Schema(config);

        table = new Table('accounts', s, serializer, docClient, logger);

        table.before('create', function (data, next) {
          return next(new Error('fail'));
        });

        table.create({email : 'foo@bar.com'}, function (err, item) {
          expect(err).to.exist;
          expect(item).to.not.exist;

          return done();
        });
      });

      it('should call after hook', function (done) {
        var config = {
          hashKey: 'email',
          schema : {
            email  : Joi.string(),
            name : Joi.string(),
            age : Joi.number()
          }
        };

        var s = new Schema(config);

        table = new Table('accounts', s, serializer, docClient, logger);

        var item = {email : 'test@test.com', name : 'Tim Test', age : 23};
        docClient.put.yields(null, {});

        serializer.serializeItem.withArgs(s, item).returns({});

        table.after('create', function (data) {
          expect(data).to.exist;

          return done();
        });

        table.create(item, function () {} );
      });
    });

    describe('#update', function () {

      it('should call before hook', function (done) {
        var config = {
          hashKey: 'email',
          schema : {
            email  : Joi.string(),
            name : Joi.string(),
            age : Joi.number()
          }
        };

        var s = new Schema(config);

        table = new Table('accounts', s, serializer, docClient, logger);

        var item = {email : 'test@test.com', name : 'Tim Test', age : 23};
        docClient.update.yields(null, {});

        serializer.serializeItem.withArgs(s, item).returns({});

        serializer.buildKey.returns({email: {S: 'test@test.com' }});
        var modified = {email : 'test@test.com', name : 'Tim Test', age : 44};
        serializer.serializeItemForUpdate.withArgs(s, 'PUT', modified).returns({});

        serializer.deserializeItem.returns(modified);
        docClient.update.yields(null, {});

        var called = false;
        table.before('update', function (data, next) {
          var attrs = _.merge({}, data, {age: 44});
          called = true;
          return next(null, attrs);
        });

        table.after('update', function () {
          expect(called).to.be.true;
          return done();
        });

        table.update(item, function () {} );
      });

      it('should return error when before hook returns error', function (done) {
        var config = {
          hashKey: 'email',
          schema : {
            email  : Joi.string(),
            name : Joi.string(),
            age : Joi.number()
          }
        };

        var s = new Schema(config);

        table = new Table('accounts', s, serializer, docClient, logger);

        table.before('update', function (data, next) {
          return next(new Error('fail'));
        });

        table.update({}, function (err) {
          expect(err).to.exist;
          err.message.should.equal('fail');

          return done();
        });
      });

      it('should call after hook', function (done) {
        var config = {
          hashKey: 'email',
          schema : {
            email  : Joi.string(),
            name : Joi.string(),
            age : Joi.number()
          }
        };

        var s = new Schema(config);

        table = new Table('accounts', s, serializer, docClient, logger);

        var item = {email : 'test@test.com', name : 'Tim Test', age : 23};
        docClient.update.yields(null, {});

        serializer.serializeItem.withArgs(s, item).returns({});

        serializer.buildKey.returns({email: {S: 'test@test.com' }});
        serializer.serializeItemForUpdate.returns({});

        serializer.deserializeItem.returns(item);
        docClient.update.yields(null, {});

        table.after('update', function () {
          return done();
        });

        table.update(item, function () {} );
      });
    });

    it('#destroy should call after hook', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email  : Joi.string(),
          name : Joi.string(),
          age : Joi.number()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      docClient.delete.yields(null, {});
      serializer.buildKey.returns({});

      table.after('destroy', function () {
        return done();
      });

      table.destroy('test@test.com', function () {} );
    });
  });
});

