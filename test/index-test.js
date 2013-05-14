'use strict';

var vogels = require('../index'),
    Table = require('../lib/table'),
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

    it('should return new instance of a table', function () {
      var Account = vogels.define('Account');

      Account.table.should.be.instanceof(Table);
    });

    it('should configure table name as accounts', function () {
      var Account = vogels.define('Account');

      Account.table.config.name.should.equal('accounts');
    });

    it('should return new account item', function () {
      var Account = vogels.define('Account');

      var acc = new Account({name: 'Test Acc'});
      acc.table.should.equal(Account.table);
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
});
