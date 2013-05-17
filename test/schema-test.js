'use strict';

var Schema = require('../lib/schema'),
    chai   = require('chai'),
    expect = chai.expect;

chai.should();

describe('schema', function () {
  var schema;

  beforeEach(function () {
    schema = new Schema();
  });

  describe('#String', function () {

    it('should do something', function () {
      schema.String('name');

      schema.attrs.should.have.keys(['name']);
      schema.attrs.name.type.type.should.equal('String');
    });

    it('should set hashkey', function () {
      schema.String('name', {hashKey: true});

      schema.hashKey.should.equal('name');
    });

    it('should set rangeKey', function () {
      schema.String('name', {rangeKey: true});

      schema.rangeKey.should.equal('name');
    });

    it('should set secondaryIndexes', function () {
      schema.String('name', {secondaryIndex: true});

      schema.secondaryIndexes.should.eql(['name']);
    });

  });

  describe('#Number', function () {
    it('should set as number', function () {
      schema.Number('age');

      schema.attrs.should.have.keys(['age']);
      schema.attrs.age.type.type.should.equal('Number');
    });
  });

  describe('#Boolean', function () {
    it('should set as boolean', function () {
      schema.Boolean('agree');

      schema.attrs.should.have.keys(['agree']);
      schema.attrs.agree.type.type.should.equal('Boolean');
    });
  });

  describe('#Date', function () {
    it('should set as date', function () {
      schema.Date('created');

      schema.attrs.should.have.keys(['created']);
      schema.attrs.created.type.type.should.equal('Date');
    });
  });

  describe('#StringSet', function () {
    it('should set as string set', function () {
      schema.StringSet('names');

      schema.attrs.should.have.keys(['names']);
      schema.attrs.names.type.type.should.equal('StringSet');
    });
  });

  describe('#NumberSet', function () {
    it('should set as number set', function () {
      schema.NumberSet('scores');

      schema.attrs.should.have.keys(['scores']);
      schema.attrs.scores.type.type.should.equal('NumberSet');
    });
  });

  describe('#validate', function () {

    it('should return no err for string', function() {
      schema.String('email', {hashKey: true});

      expect(schema.validate({email: 'foo@bar.com'})).to.be.null;
    });

    it('should return err when hashkey isnt set', function() {
      schema.String('email', {hashKey: true});
      schema.String('name');

      var err = schema.validate({name : 'foo bar'});
      expect(err).to.exist;
    });

    it('should return no error for valid date object', function() {
      schema.Date('created', {hashKey: true});

      expect(schema.validate({created: new Date()})).to.be.null;
    });

    it('should return no error when using Date.now', function() {
      schema.Date('created', {hashKey: true});

      expect(schema.validate({created: Date.now()})).to.be.null;
    });

  });
});
