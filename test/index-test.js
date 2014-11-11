'use strict';

var vogels = require('../index'),
    helper = require('./test-helper'),
    Table  = require('../lib/table'),
    chai   = require('chai'),
    expect = chai.expect,
    Joi    = require('joi'),
    sinon  = require('sinon');

chai.should();

describe('vogels', function () {

  afterEach(function () {
    vogels.reset();
  });

  describe('#define', function () {

    it('should return model', function () {
      var config = {
        hashKey : 'name',
        schema : {
          name : Joi.string()
        }
      };

      var model = vogels.define('Account', config);
      expect(model).to.not.be.nil;
    });

    it('should throw when using old api', function () {

      expect(function () {
        vogels.define('Account', function (schema) {
          schema.String('email', {hashKey: true});
        });

      }).to.throw(/define no longer accepts schema callback, migrate to new api/);
    });

    it('should have config method', function () {
      var Account = vogels.define('Account', {hashKey : 'id'});

      Account.config({tableName: 'test-accounts'});

      Account.config().name.should.equal('test-accounts');
    });

    it('should configure table name as accounts', function () {
      var Account = vogels.define('Account', {hashKey : 'id'});

      Account.config().name.should.equal('accounts');
    });

    it('should return new account item', function () {
      var Account = vogels.define('Account', {hashKey : 'id'});

      var acc = new Account({name: 'Test Acc'});
      acc.table.should.be.instanceof(Table);
    });

  });

  describe('#models', function () {

    it('should be empty', function () {
      vogels.models.should.be.empty;
    });

    it('should contain single model', function () {
      vogels.define('Account', {hashKey : 'id'});

      vogels.models.should.contain.keys('Account');
    });

  });

  describe('#model', function () {
    it('should return defined model', function () {
      var Account = vogels.define('Account', {hashKey : 'id'});

      vogels.model('Account').should.equal(Account);
    });

    it('should return null', function () {
      expect(vogels.model('Person')).to.be.null;
    });

  });

  describe('model config', function () {
    it('should configure set dynamodb driver', function () {
      var Account = vogels.define('Account', {hashKey : 'id'});

      Account.config({tableName: 'test-accounts' });

      Account.config().name.should.eq('test-accounts');
    });

    it('should configure set dynamodb driver', function () {
      var Account = vogels.define('Account', {hashKey : 'id'});

      var dynamodb = helper.realDynamoDB();
      Account.config({dynamodb: dynamodb });

      Account.docClient.should.eq(dynamodb);
    });

    it('should globally set dynamodb driver for all models', function () {
      var Account = vogels.define('Account', {hashKey : 'id'});
      var Post = vogels.define('Post', {hashKey : 'id'});

      var dynamodb = helper.realDynamoDB();
      vogels.dynamoDriver(dynamodb);

      Account.docClient.should.eq(dynamodb);
      Post.docClient.should.eq(dynamodb);
    });

    it('should continue to use globally set dynamodb driver', function () {
      var dynamodb = helper.mockDynamoDB();
      vogels.dynamoDriver(dynamodb);

      var Account = vogels.define('Account', {hashKey : 'id'});

      Account.docClient.should.eq(dynamodb);
    });

  });

  describe('#createTables', function () {
    var clock;

    beforeEach(function () {
      vogels.reset();
      var dynamodb = helper.mockDynamoDB();
      vogels.dynamoDriver(dynamodb);
      clock = sinon.useFakeTimers();
    });

    afterEach(function () {
      clock.restore();
    });

    it('should create single definied model', function (done) {
      this.timeout(0);

      var Account = vogels.define('Account', {hashKey : 'id'});

      var second = {
        Table : { TableStatus : 'PENDING'}
      };

      var third = {
        Table : { TableStatus : 'ACTIVE'}
      };

      Account.docClient.describeTable
        .onCall(0).yields(null, null)
        .onCall(1).yields(null, second)
        .onCall(2).yields(null, third);

      Account.docClient.createTable.yields(null, null);

      vogels.createTables(function (err) {
        expect(err).to.not.exist;
        expect(Account.docClient.describeTable.calledThrice).to.be.true;
        return done();
      });

      clock.tick(1200);
      clock.tick(1200);
    });

    it('should return error', function (done) {
      var Account = vogels.define('Account', {hashKey : 'id'});

      Account.docClient.describeTable.onCall(0).yields(null, null);

      Account.docClient.createTable.yields(new Error('Fail'), null);

      vogels.createTables(function (err) {
        expect(err).to.exist;
        expect(Account.docClient.describeTable.calledOnce).to.be.true;
        return done();
      });
    });

    it('should create model without callback', function (done) {
      var Account = vogels.define('Account', {hashKey : 'id'});

      var second = {
        Table : { TableStatus : 'PENDING'}
      };

      var third = {
        Table : { TableStatus : 'ACTIVE'}
      };

      Account.docClient.describeTable
        .onCall(0).yields(null, null)
        .onCall(1).yields(null, second)
        .onCall(2).yields(null, third);

      Account.docClient.createTable.yields(null, null);

      vogels.createTables();

      clock.tick(1200);
      clock.tick(1200);

      expect(Account.docClient.describeTable.calledThrice).to.be.true;
      return done();
    });

    it('should return error when waiting for table to become active', function (done) {
      var Account = vogels.define('Account', {hashKey : 'id'});

      var second = {
        Table : { TableStatus : 'PENDING'}
      };

      Account.docClient.describeTable
        .onCall(0).yields(null, null)
        .onCall(1).yields(null, second)
        .onCall(2).yields(new Error('fail'));

      Account.docClient.createTable.yields(null, null);

      vogels.createTables(function (err) {
        expect(err).to.exist;
        expect(Account.docClient.describeTable.calledThrice).to.be.true;
        return done();
      });

      clock.tick(1200);
      clock.tick(1200);
    });

  });

});
