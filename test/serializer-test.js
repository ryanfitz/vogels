'use strict';

var serializer = require('../lib/serializer'),
    chai = require('chai'),
    expect = chai.expect,
    Schema = require('../lib/schema');

chai.should();

describe('Serializer', function () {
  var schema;

  beforeEach(function () {
    schema = new Schema();
  });

  describe('#buildKeys', function () {

    it('should handle string hash key', function () {
      schema.String('email', {hashKey: true});

      var keys = serializer.buildKey('test@test.com', null, schema);

      keys.should.eql({email: {S: 'test@test.com'}});
    });

    it('should handle number hash key', function () {
      schema.Number('year', {hashKey: true});

      var keys = serializer.buildKey(1999, null, schema);

      keys.should.eql({year: {N: '1999'}});
    });

    it('should handle date hash key', function () {
      schema.Date('timestamp', {hashKey: true});

      var d = new Date();
      var keys = serializer.buildKey(d, null, schema);

      keys.should.eql({timestamp: {S: d.toISOString()}});
    });

    it('should handle string hash and range key', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.String('slug');

      var keys = serializer.buildKey('Tim Tester', 'test@test.com', schema);

      keys.should.eql({name: {S: 'Tim Tester'}, email: {S : 'test@test.com'}});
    });

    it('should handle number hash and range key', function () {
      schema.Number('year', {hashKey: true});
      schema.Number('num', {rangeKey: true});

      var keys = serializer.buildKey(1988, 1.4, schema);

      keys.should.eql({year: {N: '1988'}, num: {N : '1.4'}});
    });

    it('should handle object containing the hash key', function () {
      schema.Number('year', {hashKey: true});
      schema.String('name', {rangeKey: true});
      schema.String('slug');

      var keys = serializer.buildKey({year: 1988, name : 'Joe'}, null, schema);

      keys.should.eql({year: {N: '1988'}, name: {S: 'Joe'}});
    });

  });
  describe('#deserializeKeys', function () {

    it('should handle string hash key', function () {
      schema.String('email', {hashKey: true});
      schema.String('name');

      var keys = serializer.deserializeKeys(schema, {email : {S : 'test@example.com'}, name : {S: 'Foo Bar'}});

      keys.should.eql({email: 'test@example.com'});
    });

    it('should handle range key', function () {
      schema.String('email', {hashKey: true});
      schema.Number('age', {rangeKey: true});
      schema.String('name');

      var serializedItem = {email : {S : 'test@example.com'}, age : {N : '22'}, name : {S: 'Foo Bar'}};
      var keys = serializer.deserializeKeys(schema, serializedItem);

      keys.should.eql({email: 'test@example.com', age: 22});
    });

  });

  describe('#serializeItem', function () {
    it('should serialize string attribute', function () {
      schema.String('name');

      var item = serializer.serializeItem(schema, {name: 'Tim Tester'});

      item.should.eql({name: {S: 'Tim Tester'}});
    });

    it('should serialize number attribute', function () {
      schema.Number('age');

      var item = serializer.serializeItem(schema, {age: 21});

      item.should.eql({age: {N: '21'}});
    });

    it('should serialize boolean attribute', function () {
      schema.Boolean('agree');

      serializer.serializeItem(schema, {agree: true}).should.eql({agree: {N: '1'}});
      serializer.serializeItem(schema, {agree: 'true'}).should.eql({agree: {N: '1'}});

      serializer.serializeItem(schema, {agree: false}).should.eql({agree: {N: '0'}});
      serializer.serializeItem(schema, {agree: 'false'}).should.eql({agree: {N: '0'}});
      //serializer.serializeItem(schema, {agree: null}).should.eql({agree: {N: '0'}});
      serializer.serializeItem(schema, {agree: 0}).should.eql({agree: {N: '0'}});
    });

    it('should serialize date attribute', function () {
      schema.Date('time');

      var d = new Date();
      var item = serializer.serializeItem(schema, {time: d});

      item.should.eql({time: {S: d.toISOString()}});
    });

    it('should serialize string set attribute', function () {
      schema.StringSet('names');

      var item = serializer.serializeItem(schema, {names: ['Tim', 'Steve', 'Bob']});

      item.should.eql({names: {SS: ['Tim', 'Steve', 'Bob']}});
    });

    it('should serialize single string set attribute', function () {
      schema.StringSet('names');

      var item = serializer.serializeItem(schema, {names: 'Tim'});

      item.should.eql({names: {SS: ['Tim']}});
    });

    it('should number set attribute', function () {
      schema.NumberSet('scores');

      var item = serializer.serializeItem(schema, {scores: [2, 4, 6, 8]});

      item.should.eql({scores: {NS: ['2', '4', '6', '8']}});
    });

    it('should single number set attribute', function () {
      schema.NumberSet('scores');

      var item = serializer.serializeItem(schema, {scores: 2});

      item.should.eql({scores: {NS: ['2']}});
    });

    it('should serialize uuid attribute', function () {
      schema.UUID('id');

      var id = '1234-5123-2342-1234';
      var item = serializer.serializeItem(schema, {id: id});

      item.should.eql({id: {S: id}});
    });

    it('should serialize TimeUUId attribute', function () {
      schema.TimeUUID('timeid');

      var timeid = '1234-5123-2342-1234';
      var item = serializer.serializeItem(schema, {timeid: timeid});

      item.should.eql({timeid: {S: timeid}});
    });

    it('should return null', function () {
      schema.String('email');
      schema.NumberSet('scores');

      var item = serializer.serializeItem(schema, null);

      expect(item).to.be.null;
    });

    it('should convert string set to a string', function () {
      schema.StringSet('names');

      var item = serializer.serializeItem(schema, {names: 'Bob'}, {convertSets: true});

      item.should.eql({names: {S: 'Bob'}});
    });
  });

  describe('#deserializeItem', function () {
    it('should parse string attribute', function () {
      schema.String('name');

      var itemResp = {name : {S: 'Tim Tester'} };

      var item = serializer.deserializeItem(schema, itemResp);

      item.name.should.equal('Tim Tester');
    });

    it('should parse number attribute', function () {
      schema.Number('age');

      var itemResp = {age : {N: '18'} };

      var item = serializer.deserializeItem(schema, itemResp);

      item.age.should.equal(18);
    });

    it('should parse number attribute', function () {
      schema.Date('created');

      var itemResp = {created : {S: '2013-05-15T21:47:28.479Z'} };

      var item = serializer.deserializeItem(schema, itemResp);

      item.created.should.eql(new Date('2013-05-15T21:47:28.479Z'));
    });

    it('should parse boolean attribute', function () {
      schema.Boolean('agree');

      serializer.deserializeItem(schema, {agree: {N: '1'}}).agree.should.be.true;
      serializer.deserializeItem(schema, {agree: {N: '0'}}).agree.should.be.false;

      serializer.deserializeItem(schema, {agree: {S: 'true'}}).agree.should.be.true;
      serializer.deserializeItem(schema, {agree: {S: 'false'}}).agree.should.be.false;
    });

    it('should parse string set attribute', function () {
      schema.StringSet('names');

      var itemResp = {names : {SS: ['Bob', 'Joe', 'Tim']} };

      var item = serializer.deserializeItem(schema, itemResp);

      item.names.should.eql(['Bob', 'Joe', 'Tim']);
    });

    it('should parse number set attribute', function () {
      schema.NumberSet('nums');

      var itemResp = {nums : {NS: ['18', '22', '23']} };

      var item = serializer.deserializeItem(schema, itemResp);

      item.nums.should.eql([18, 22, 23]);
    });

    it('should return null', function () {
      schema.String('email');
      schema.NumberSet('nums');

      var item = serializer.deserializeItem(schema, null);

      expect(item).to.be.null;
    });

    it('should parse uuid attribute', function () {
      schema.UUID('id');

      var itemResp = {id : {S: '1234-5678-9012'} };

      var item = serializer.deserializeItem(schema, itemResp);

      item.id.should.equal('1234-5678-9012');
    });

    it('should parse time uuid attribute', function () {
      schema.TimeUUID('stamp');

      var itemResp = {stamp : {S: '1234-5678-9012'} };

      var item = serializer.deserializeItem(schema, itemResp);

      item.stamp.should.equal('1234-5678-9012');
    });

  });

  describe('#serializeItemForUpdate', function () {
    it('should serialize string attribute', function () {
      schema.String('name');

      var item = serializer.serializeItemForUpdate(schema, 'PUT', {name: 'Tim Tester'});

      item.should.eql({ name: {Action: 'PUT', Value: {S: 'Tim Tester'} }});
    });

    it('should serialize number attribute', function () {
      schema.Number('age');

      var item = serializer.serializeItemForUpdate(schema, 'PUT', {age: 25});

      item.should.eql({ age: {Action: 'PUT', Value: {N: '25'} }});
    });

    it('should serialize three attributes', function () {
      schema.String('name');
      schema.Number('age');
      schema.NumberSet('scores');

      var attr = {name: 'Tim Test', age: 25, scores: [94, 92, 100]};
      var item = serializer.serializeItemForUpdate(schema, 'PUT', attr);

      item.should.eql({
        name   : {Action : 'PUT', Value : {S  : 'Tim Test'}},
        age    : {Action : 'PUT', Value : {N  : '25'} },
        scores : {Action : 'PUT', Value : {NS : ['94', '92', '100']} }
      });
    });

    it('should serialize null value to a DELETE action', function () {
      schema.String('name');
      schema.Number('age');

      var item = serializer.serializeItemForUpdate(schema, 'PUT', {age: null, name : 'Foo Bar'});

      item.should.eql({
        name: {Action: 'PUT', Value: {S: 'Foo Bar'} },
        age:  {Action: 'DELETE'}
      });
    });

    it('should not serialize hashkey attribute', function () {
      schema.String('email', {hashKey: true});
      schema.String('name');

      var item = serializer.serializeItemForUpdate(schema, 'PUT', {email: 'test@test.com', name: 'Tim Tester'});

      item.should.eql({ name: {Action: 'PUT', Value: {S: 'Tim Tester'} }});
    });

    it('should not serialize hashkey and rangeKey attributes', function () {
      schema.String('email', {hashKey: true});
      schema.String('range', {rangeKey: true});
      schema.String('name');

      var item = serializer.serializeItemForUpdate(schema, 'PUT', {email: 'test@test.com', range: 'FOO', name: 'Tim Tester'});

      item.should.eql({ name: {Action: 'PUT', Value: {S: 'Tim Tester'} }});
    });


  });
});
