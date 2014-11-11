'use strict';

var helper = require('./test-helper'),
    chai   = require('chai'),
    expect = chai.expect,
    Schema = require('../lib/schema'),
    Item   = require('../lib/item'),
    batch  = require('../lib/batch'),
    Serializer = require('../lib/serializer'),
    Joi    = require('joi'),
    _      = require('lodash');

describe('Batch', function () {
  var serializer,
      table;

  beforeEach(function () {
    serializer = helper.mockSerializer(),

    table = helper.mockTable();
    table.serializer = Serializer;
    table.tableName = function () {
      return 'accounts';
    };

    var config = {
      hashKey: 'name',
      rangeKey: 'email',
      schema : {
        name : Joi.string(),
        email : Joi.string(),
        age : Joi.number()
      }
    };

    table.schema = new Schema(config);
  });

  describe('#getItems', function () {

    it('should get items by hash key', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email : Joi.string(),
          name : Joi.string(),
        }
      };

      table.schema = new Schema(config);

      var response = {
        Responses : {
          accounts : [
            {email : 'test@test.com', name : 'Tim Tester'},
            {email : 'foo@example.com', name : 'Foo Bar'}
          ]
        }
      };

      var expectedRequest = {
        RequestItems : {
          accounts : {
            Keys : [
              {email : 'test@test.com'},
              {email : 'foo@example.com'}
            ]
          }
        }
      };

      var item1 = {email: 'test@test.com', name : 'Tim Tester'};
      table.runBatchGetItems.withArgs(expectedRequest).yields(null, response);

      table.initItem.returns(new Item(item1));

      batch(table, Serializer).getItems(['test@test.com', 'foo@example.com'], function (err, items) {
        items.should.have.length(2);
        items[0].get('email').should.equal('test@test.com');

        done();
      });
    });

    it('should get items by hash and range key', function (done) {
      var key1 = {email: 'test@test.com', name : 'Tim Tester'};
      var key2 = {email: 'foo@example.com', name : 'Foo Bar'};

      var response = {
        Responses : {
          accounts : [
            {email : 'test@test.com', name : 'Tim Tester'},
            {email : 'foo@example.com', name : 'Foo Bar'}
          ]
        }
      };

      var expectedRequest = {
        RequestItems : {
          accounts : {
            Keys : [
              {email : key1.email, name : key1.name},
              {email : key2.email, name : key2.name}
            ]
          }
        }
      };

      var item1 = {email: 'test@test.com', name : 'Tim Tester', age: 22};
      table.runBatchGetItems.withArgs(expectedRequest).yields(null, response);

      table.initItem.returns(new Item(item1));

      batch(table, Serializer).getItems([key1, key2], function (err, items) {
        items.should.have.length(2);
        items[0].get('email').should.equal('test@test.com');

        done();
      });
    });

    it('should not modify passed in keys', function (done) {
      var keys = _.map(_.range(300), function (num) {
        var key = {email: 'test' + num + '@test.com', name : 'Test ' + num};
        serializer.buildKey.withArgs(key).returns({email : {S : key.email}, name : {S: key.name}});

        return key;
      });

      var item1 = {email: 'test@test.com', name : 'Tim Tester', age: 22};
      table.runBatchGetItems.yields(null, {});
      serializer.deserializeItem.returns(item1);

      table.initItem.returns(new Item(item1));

      batch(table, serializer).getItems(keys, function () {

        _.each(_.range(300), function (num) {
          var key = {email: 'test' + num + '@test.com', name : 'Test ' + num};
          keys[num].should.eql(key);
        });

        done();
      });
    });

    it('should get items by hash key with consistent read', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email : Joi.string(),
          name : Joi.string(),
        }
      };

      table.schema = new Schema(config);

      var response = {
        Responses : {
          accounts : [
            {email : 'test@test.com', name :'Tim Tester'},
            {email : 'foo@example.com', name : 'Foo Bar'}
          ]
        }
      };

      var expectedRequest = {
        RequestItems : {
          accounts : {
            Keys : [
              {email : 'test@test.com'},
              {email : 'foo@example.com'}
            ],
            ConsistentRead : true
          }
        }
      };

      var item1 = {email: 'test@test.com', name : 'Tim Tester'};
      table.runBatchGetItems.withArgs(expectedRequest).yields(null, response);

      table.initItem.returns(new Item(item1));

      batch(table, Serializer).getItems(['test@test.com', 'foo@example.com'], {ConsistentRead : true}, function (err, items) {
        items.should.have.length(2);
        items[0].get('email').should.equal('test@test.com');

        done();
      });
    });

    it('should get items by hash key with projection expression', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email : Joi.string(),
          name : Joi.string(),
        }
      };

      table.schema = new Schema(config);

      var response = {
        Responses : {
          accounts : [
            {email : 'test@test.com', name :'Tim Tester'},
            {email : 'foo@example.com', name : 'Foo Bar'}
          ]
        }
      };

      var expectedRequest = {
        RequestItems : {
          accounts : {
            Keys : [
              {email : 'test@test.com'},
              {email : 'foo@example.com'}
            ],
            ProjectionExpression : '#name, #e',
            ExpressionAttributeNames : { '#name' : 'name', '#email' : 'email'}
          }
        }
      };

      var item1 = {email: 'test@test.com', name : 'Tim Tester'};
      table.runBatchGetItems.withArgs(expectedRequest).yields(null, response);

      table.initItem.returns(new Item(item1));

      var opts = {
        ProjectionExpression : '#name, #e',
        ExpressionAttributeNames : { '#name' : 'name', '#email' : 'email'}
      };

      batch(table, Serializer).getItems(['test@test.com', 'foo@example.com'], opts, function (err, items) {
        items.should.have.length(2);
        items[0].get('email').should.equal('test@test.com');

        done();
      });
    });

    it('should get items when encounters retryable excpetion', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email : Joi.string(),
          name : Joi.string(),
        }
      };

      table.schema = new Schema(config);

      var response = {
        Responses : {
          accounts : [
            {email : 'test@test.com', name :'Tim Tester'},
            {email : 'foo@example.com', name : 'Foo Bar'}
          ]
        }
      };

      var item1 = {email: 'test@test.com', name : 'Tim Tester'};

      var err = new Error('RetryableException');
      err.retryable = true;

      table.runBatchGetItems
        .onCall(0).yields(err)
        .onCall(1).yields(null, response);

      table.initItem.returns(new Item(item1));

      batch(table, Serializer).getItems(['test@test.com', 'foo@example.com'], function (err, items) {
        expect(err).to.not.exist;

        expect(table.runBatchGetItems.calledTwice).to.be.true;
        items.should.have.length(2);
        items[0].get('email').should.equal('test@test.com');

        done();
      });
    });

    it('should return error', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email : Joi.string(),
          name : Joi.string(),
        }
      };

      table.schema = new Schema(config);

      var err = new Error('Error');
      table.runBatchGetItems.onCall(0).yields(err);

      batch(table, Serializer).getItems(['test@test.com', 'foo@example.com'], function (err, items) {
        expect(err).to.exist;
        expect(items).to.not.exist;

        expect(table.runBatchGetItems.calledOnce).to.be.true;
        done();
      });
    });
  });

});
