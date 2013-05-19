'use strict';

var helper = require('./test-helper'),
    _      = require('lodash'),
    Table  = require('../lib/table'),
    Schema = require('../lib/schema'),
    Query  = require('../lib//query'),
    Scan   = require('../lib//scan'),
    Item   = require('../lib/item');

describe('table', function () {
  var schema,
      table,
      serializer,
      dynamodb;

  beforeEach(function () {
    schema = new Schema();
    serializer = helper.mockSerializer(),
    dynamodb = helper.mockDynamoDB();
  });

  describe('#get', function () {

    it('should get item by hash key', function (done) {
      schema.String('email', {hashKey: true});
      schema.String('name');

      table = new Table('accounts', schema, serializer, dynamodb);

      var request = {
        TableName: 'accounts',
        Key : {
          email : {S : 'test@test.com'}
        }
      };

      var resp = {
        Item : {email: {S : 'test@test.com'}, name: {S: 'test dude'}}
      };

      dynamodb.getItem.withArgs(request).yields(null, resp);

      serializer.buildKey.returns({email: resp.Item.email});

      serializer.deserializeItem.withArgs(schema, resp.Item).returns({email : 'test@test.com', name : 'test dude'});

      table.get('test@test.com', function (err, account) {
        account.should.be.instanceof(Item);
        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('test dude');

        done();
      });
    });
  });

  describe('#create', function () {

    it('should create valid item', function (done) {
      schema.String('email', {hashKey: true});
      schema.String('name');
      schema.Number('age');

      table = new Table('accounts', schema, serializer, dynamodb);

      var request = {
        TableName: 'accounts',
        Item : {
          email : {S : 'test@test.com'},
          name  : {S : 'Tim Test'},
          age   : {N : '23'}
        }
      };

      var item = {email : 'test@test.com', name : 'Tim Test', age : 23};
      dynamodb.putItem.withArgs(request).yields(null, {});

      serializer.serializeItem.withArgs(schema, item).returns(request.Item);

      table.create(item, function (err, account) {
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('Tim Test');

        done();
      });
    });

    it('should call apply defaults', function (done) {
      schema.String('email', {hashKey: true});
      schema.String('name', {default: 'Foo'});
      schema.Number('age');

      table = new Table('accounts', schema, serializer, dynamodb);

      var request = {
        TableName: 'accounts',
        Item : {
          email : {S : 'test@test.com'},
          name  : {S : 'Foo'},
          age   : {N : '23'}
        }
      };

      var item = {email : 'test@test.com', name : 'Foo', age : 23};
      dynamodb.putItem.withArgs(request).yields(null, {});

      serializer.serializeItem.withArgs(schema, item).returns(request.Item);

      table.create({email : 'test@test.com', age: 23}, function (err, account) {
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('Foo');

        done();
      });
    });

  });

  describe('#update', function () {

    it('should update valid item', function (done) {
      schema.String('email', {hashKey: true});
      schema.String('name');
      schema.Number('age');

      table = new Table('accounts', schema, serializer, dynamodb);

      var request = {
        TableName: 'accounts',
        Key : {
          email : {S : 'test@test.com'}
        },
        AttributeUpdates : {
          email : {Action : 'PUT', Value: {S : 'test@test.com'}},
          name  : {Action : 'PUT', Value: {S : 'Tim Test'}},
          age   : {Action : 'PUT', Value: {N : '23'}}
        },
        ReturnValues: 'ALL_NEW'
      };

      var returnedAttributes = {
        email  : {S : 'test@test.com'},
        name   : {S : 'Tim Test'},
        age    : {N : '25'},
        scores : {NS : ['97', '86']}
      };

      var item = {email : 'test@test.com', name : 'Tim Test', age : 23};

      serializer.buildKey.returns(request.Key);
      serializer.serializeItemForUpdate.withArgs(schema, 'PUT', item).returns(request.AttributeUpdates);

      var returnedItem = _.merge({}, item, {scores: [97, 86]});
      serializer.deserializeItem.withArgs(schema, returnedAttributes).returns(returnedItem);
      dynamodb.updateItem.withArgs(request).yields(null, {Attributes: returnedAttributes});

      table.update(item, function (err, account) {
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('Tim Test');
        account.get('age').should.equal(23);
        account.get('scores').should.eql([97, 86]);

        done();
      });
    });
  });

  describe('#query', function () {

    it('should return query object', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});

      table = new Table('accounts', schema, serializer, dynamodb);

      table.query('Bob').should.be.instanceof(Query);
    });
  });

  describe('#scan', function () {

    it('should return scan object', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});

      table = new Table('accounts', schema, serializer, dynamodb);

      table.scan().should.be.instanceof(Scan);
    });
  });

  describe('#destroy', function () {

    it('should destroy valid item', function (done) {
      schema.String('email', {hashKey: true});
      schema.String('name');
      schema.Number('age');

      table = new Table('accounts', schema, serializer, dynamodb);

      var request = {
        TableName: 'accounts',
        Key : {
          email : {S : 'test@test.com'}
        }
      };

      dynamodb.deleteItem.yields(null, {});

      serializer.buildKey.returns(request.Key);

      table.destroy('test@test.com', function () {
        serializer.buildKey.calledWith('test@test.com', null, schema).should.be.true;
        dynamodb.deleteItem.calledWith(request).should.be.true;

        done();
      });
    });
  });

});
