'use strict';

var vogels = require('../index'),
    helper = require('./test-helper'),
    Table  = require('../lib/table'),
    Schema = require('../lib/schema'),
    chai   = require('chai'),
    expect = chai.expect;

chai.should();

describe('vogels', function () {

  afterEach(function () {
    vogels.reset();
  });

  describe('#define', function () {

    it('should invoke callback with instance of a schema', function (done) {
      vogels.define('Account', function (schema) {
        schema.should.be.instanceof(Schema);
        done();
      });
    });

    it('should have config method', function () {
      var Account = vogels.define('Account');

      Account.config({tableName: 'test-accounts'});

      Account.config().name.should.equal('test-accounts');
    });

    it('should configure table name as accounts', function () {
      var Account = vogels.define('Account');

      Account.config().name.should.equal('accounts');
    });

    it('should return new account item', function () {
      var Account = vogels.define('Account');

      var acc = new Account({name: 'Test Acc'});
      acc.table.should.be.instanceof(Table);
    });

    it('should set table name', function() {
      var d = new Date(),
          Account = vogels.define('Account', function (schema) {
        schema.String('email', {hashKey: true});
        schema.String('name');
        schema.Number('age');
        schema.Date('created', {default: Date.now});

        schema.tableName = function () {
          return ['Accounts', d.getFullYear(), d.getMonth(), d.getDate()].join('_');
        };
      });
      Account.config().name.should.equal(['Accounts', d.getFullYear(), d.getMonth(), d.getDate()].join('_'));
    });
  });

  describe('#models', function () {

    it('should be empty', function () {
      vogels.models.should.be.empty;
    });

    it('should contain single model', function () {
      vogels.define('Account');

      vogels.models.should.contain.keys('Account');
    });

  });

  describe('#model', function () {
    it('should return defined model', function () {
      var Account = vogels.define('Account');

      vogels.model('Account').should.equal(Account);
    });

    it('should return null', function () {
      expect(vogels.model('Person')).to.be.null;
    });

  });

  describe('model config', function () {
    it('should configure set dynamodb driver', function () {
      var Account = vogels.define('Account');

      Account.config({tableName: 'test-accounts' });

      Account.config().name.should.eq('test-accounts');
    });

    it('should configure set dynamodb driver', function () {
      var Account = vogels.define('Account');

      var dynamodb = helper.mockDynamoDB();
      Account.config({dynamodb: dynamodb });

      Account.dynamodb.should.eq(dynamodb);
    });

    it('should globally set dynamodb driver for all models', function () {
      var Account = vogels.define('Account');
      var Post = vogels.define('Post');

      var dynamodb = helper.mockDynamoDB();
      vogels.dynamoDriver(dynamodb);

      Account.dynamodb.should.eq(dynamodb);
      Post.dynamodb.should.eq(dynamodb);
    });

    it('should continue to use globally set dynamodb driver', function () {
      var dynamodb = helper.mockDynamoDB();
      vogels.dynamoDriver(dynamodb);

      var Account = vogels.define('Account');

      Account.dynamodb.should.eq(dynamodb);
    });

  });
});
