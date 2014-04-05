'use strict';

var Schema = require('../lib/schema'),
    chai   = require('chai'),
    expect = chai.expect,
    sinon  = require('sinon');

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
      schema.attrs.name.type._type.should.equal('string');
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
      schema.attrs.age.type._type.should.equal('number');
    });
  });

  describe('#Boolean', function () {
    it('should set as boolean', function () {
      schema.Boolean('agree');

      schema.attrs.should.have.keys(['agree']);
      schema.attrs.agree.type._type.should.equal('boolean');
    });
  });

  describe('#Date', function () {
    it('should set as date', function () {
      schema.Date('created');

      schema.attrs.should.have.keys(['created']);
      schema.attrs.created.type._type.should.equal('date');
    });
  });

  describe('#StringSet', function () {
    it('should set as string set', function () {
      schema.StringSet('names');

      schema.attrs.should.have.keys(['names']);
      schema.attrs.names.type._type.should.equal('stringSet');
    });
  });

  describe('#NumberSet', function () {
    it('should set as number set', function () {
      schema.NumberSet('scores');

      schema.attrs.should.have.keys(['scores']);
      schema.attrs.scores.type._type.should.equal('numberSet');
    });
  });

  describe('#UUID', function () {
    it('should set as uuid with default uuid function', function () {
      schema.UUID('id');

      schema.attrs.should.have.keys(['id']);
      schema.attrs.id.options.default.should.exist;
      schema.attrs.id.type._type.should.equal('uuid');
    });

    it('should set as uuid with default as given value', function () {
      schema.UUID('id', {default : '123'});

      schema.attrs.should.have.keys(['id']);
      schema.attrs.id.options.default.should.equal('123');
      schema.attrs.id.type._type.should.equal('uuid');
    });
  });

  describe('#TimeUUID', function () {
    it('should set as TimeUUID with default v1 uuid function', function () {
      schema.TimeUUID('timeid');

      schema.attrs.should.have.keys(['timeid']);
      schema.attrs.timeid.options.default.should.exist;
      schema.attrs.timeid.type._type.should.equal('timeuuid');
    });

    it('should set as uuid with default as given value', function () {
      schema.TimeUUID('stamp', {default : '123'});

      schema.attrs.should.have.keys(['stamp']);
      schema.attrs.stamp.options.default.should.equal('123');
      schema.attrs.stamp.type._type.should.equal('timeuuid');
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

  describe('#defaults', function () {
    it('should return default option set on hashkey', function () {
      schema.String('email', {hashKey: true, default: 'foo@bar.com'});

      schema.defaults().should.have.keys(['email']);
    });

    it('should return attributes that have defautls', function () {
      schema.String('email', {hashKey: true});
      schema.String('name', {default: 'Foo Bar'});
      schema.Number('age', {default: 3});
      schema.Number('posts', {default: 0});
      schema.Boolean('terms', {default: false});

      schema.defaults().should.have.keys(['name', 'age', 'posts', 'terms']);
    });

    it('should return empty object when no defaults exist', function () {
      schema.String('email', {hashKey: true});
      schema.String('name');
      schema.Number('age');

      schema.defaults().should.be.empty;
    });
  });

  describe('#applyDefaults', function () {
    it('should apply default values', function () {
      schema.String('email', {hashKey: true});
      schema.String('name', {default: 'Foo Bar'});
      schema.Number('age', {default: 3});

      var d = schema.applyDefaults({email: 'foo@bar.com'});

      d.email.should.equal('foo@bar.com');
      d.name.should.equal('Foo Bar');
      d.age.should.equal(3);
    });

    it('should return result of default functions', function () {
      var clock = sinon.useFakeTimers(Date.now());

      schema.String('email', {hashKey: true});
      schema.Date('created', {default: Date.now});

      var d = schema.applyDefaults({email: 'foo@bar.com'});

      d.created.should.equal(Date.now());

      clock.restore();
    });

    it('should modify passed in data', function () {
      schema.String('email', {hashKey: true});
      schema.String('name', {default: 'Foo Bar'});
      schema.Number('age', {default: 3});

      var data = {email : 'test@example.com'};
      schema.applyDefaults(data);

      data.email.should.equal('test@example.com');
      data.name.should.equal('Foo Bar');
      data.age.should.equal(3);
    });

    it('should modify anything when no defaults are set', function () {
      schema.String('email');
      schema.String('name');
      schema.Number('age');

      var d = schema.applyDefaults({email: 'foo@bar.com'});

      d.email.should.equal('foo@bar.com');
      expect(d.name).to.not.exist;
      expect(d.age).to.not.exist;
    });
  });

  describe('#globalIndex', function () {

    it('should set globalIndexes', function () {
      schema.String('userId', {hashKey: true});
      schema.String('gameTitle', {rangeKey: true});
      schema.Number('topScore');

      schema.globalIndex('GameTitleIndex', {hashKey: 'gameTitle', rangeKey : 'topScore'});

      schema.globalIndexes.should.include.keys('GameTitleIndex');
    });

  });
});
