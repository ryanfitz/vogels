'use strict';

var helper = require('./test-helper'),
    Schema = require('../lib/schema'),
    Item   = require('../lib/item'),
    batch  = require('../lib/batch'),
    sinon  = require('sinon'),
    should = require('chai').should(),
    _      = require('lodash');

describe('Batch', function () {
  var schema,
  serializer,
  table;

  beforeEach(function () {
    schema = new Schema();
    serializer = helper.mockSerializer(),

    table = helper.mockTable();
    table.config = { name: 'accounts' };
    table.schema = schema;
  });

  describe('#getItems', function () {

    it('should get items by hash key', function (done) {
      schema.String('email', {hashKey: true});
      schema.String('name');

      serializer.buildKey.withArgs('test@test.com').returns({email : {S : 'test@test.com'}});
      serializer.buildKey.withArgs('foo@example.com').returns({email : {S : 'foo@example.com'}});

      var response = {
        Responses : {
          accounts : [
            {email : {S: 'test@test.com'}, name : {S : 'Tim Tester'}},
            {email : {S: 'foo@example.com'}, name : {S : 'Foo Bar'}}
          ]
        }
      };

      var expectedRequest = {
        RequestItems : {
          accounts : {
            Keys : [
              {email : {S : 'test@test.com'}},
              {email : {S : 'foo@example.com'}}
            ]
          }
        }
      };

      var item1 = {email: 'test@test.com', name : 'Tim Tester'};
      table.runBatchGetItems.withArgs(expectedRequest).yields(null, response);
      serializer.deserializeItem.returns(item1);

      table.initItem.returns(new Item(item1));

      batch(table, serializer).getItems(['test@test.com', 'foo@example.com'], function (err, items) {
        items.should.have.length(2);
        items[0].get('email').should.equal('test@test.com');

        done();
      });
    });

    it('should replay unprocessed items', function(done) {
      var isFirstCall = true;
      table.runBatchGetItems = sinon.spy(function(request, callback) {
        var response = {
          Responses: {
            accounts: request.RequestItems.accounts.Keys
          }
        };

        // If this is the first call, make the last item fail
        if (isFirstCall === true) {
          response.UnprocessedKeys = {
            accounts: {
              Keys: [_.last(response.Responses.accounts)]
            }
          };
          response.Responses.accounts = _.initial(response.Responses.accounts);
        }
        isFirstCall = false;
        callback(null, response);
      });

      var emails = ['test@test.com', 'foo@example.com', 'last@example.com'];
      _.each(emails, function(email) {
        serializer.buildKey.withArgs(email).returns({email : {S : email}});
      });

      batch(table, serializer).getItems(['test@test.com', 'foo@example.com', 'last@example.com'], function(err, items) {
        if (err) return done(err);

        table.runBatchGetItems.calledTwice.should.equal(true);

        var requestKeys = table.runBatchGetItems.getCall(0).args[0].RequestItems.accounts.Keys;
        requestKeys.length.should.equal(3);

        requestKeys = table.runBatchGetItems.getCall(1).args[0].RequestItems.accounts.Keys;
        requestKeys.length.should.equal(1);
        requestKeys[0].email['S'].should.eq('last@example.com');

        done();
      });
    });

    it('should get items by hash and range key', function (done) {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.Number('age');

      var key1 = {email: 'test@test.com', name : 'Tim Tester'};
      var key2 = {email: 'foo@example.com', name : 'Foo Bar'};

      serializer.buildKey.withArgs(key1).returns({email : {S : key1.email}, name : {S: key1.name}});
      serializer.buildKey.withArgs(key2).returns({email : {S : key2.email}, name : {S: key2.name}});

      var response = {
        Responses : {
          accounts : [
            {email : {S: 'test@test.com'}, name : {S : 'Tim Tester'}},
            {email : {S: 'foo@example.com'}, name : {S : 'Foo Bar'}}
          ]
        }
      };

      var expectedRequest = {
        RequestItems : {
          accounts : {
            Keys : [
              {email : {S : key1.email}, name : {S: key1.name}},
              {email : {S : key2.email}, name : {S: key2.name}}
            ]
          }
        }
      };

      var item1 = {email: 'test@test.com', name : 'Tim Tester', age: 22};
      table.runBatchGetItems.withArgs(expectedRequest).yields(null, response);
      serializer.deserializeItem.returns(item1);

      table.initItem.returns(new Item(item1));

      batch(table, serializer).getItems([key1, key2], function (err, items) {
        items.should.have.length(2);
        items[0].get('email').should.equal('test@test.com');

        done();
      });
    });

    it('should not modify passed in keys', function (done) {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.Number('age');

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

  });

});
