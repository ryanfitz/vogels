'use strict';

var expressions = require('../lib/expressions'),
    chai        = require('chai'),
    expect      = chai.expect,
    Schema      = require('../lib/schema'),
    Joi         = require('joi');
    //_         = require('lodash');

chai.should();

describe('expressions', function () {

  describe('#parse', function () {

    it('should parse single SET action', function () {
      var out = expressions.parse('SET foo = :x');

      expect(out).to.eql({
        SET : ['foo = :x'],
        ADD : null,
        REMOVE : null,
        DELETE : null
      });
    });

    it('should parse multiple SET actions', function () {
      var out = expressions.parse('SET num = num + :n,Price = if_not_exists(Price, 100), #pr.FiveStar = list_append(#pr.FiveStar, :r)');

      expect(out).to.eql({
        SET : ['num = num + :n', 'Price = if_not_exists(Price, 100)', '#pr.FiveStar = list_append(#pr.FiveStar, :r)'],
        ADD : null,
        REMOVE : null,
        DELETE : null
      });
    });

    it('should parse ADD action', function () {
      var out = expressions.parse('ADD num :y');

      expect(out).to.eql({
        SET : null,
        ADD : ['num :y'],
        REMOVE : null,
        DELETE : null
      });
    });

    it('should parse REMOVE action', function () {
      var out = expressions.parse('REMOVE Title, RelatedItems[2], Pictures.RearView');

      expect(out).to.eql({
        SET : null,
        ADD : null,
        REMOVE : ['Title', 'RelatedItems[2]', 'Pictures.RearView'],
        DELETE : null
      });
    });


    it('should parse DELETE action', function () {
      var out = expressions.parse('DELETE color :c');

      expect(out).to.eql({
        SET : null,
        ADD : null,
        REMOVE : null,
        DELETE : ['color :c']
      });
    });

    it('should parse ADD and SET actions', function () {
      var out = expressions.parse('ADD num :y SET name = :n');

      expect(out).to.eql({
        SET : ['name = :n'],
        ADD : ['num :y'],
        REMOVE : null,
        DELETE : null
      });
    });

    it('should parse multiple actions', function () {
      var out = expressions.parse('SET list[0] = :val1 REMOVE #m.nestedField1, #m.nestedField2 ADD aNumber :val2, anotherNumber :val3 DELETE aSet :val4');

      expect(out).to.eql({
        SET : ['list[0] = :val1'],
        ADD : ['aNumber :val2', 'anotherNumber :val3'],
        REMOVE : ['#m.nestedField1', '#m.nestedField2'],
        DELETE : ['aSet :val4']
      });
    });

    it('should return null actions when given null', function () {
      var out = expressions.parse(null);

      expect(out).to.eql({
        SET : null,
        ADD : null,
        REMOVE : null,
        DELETE : null
      });
    });

    it('should return null actions when given empty string', function () {
      var out = expressions.parse('');

      expect(out).to.eql({
        SET : null,
        ADD : null,
        REMOVE : null,
        DELETE : null
      });
    });
  });

  describe('#serializeUpdateExpression', function () {
    var schema;

    beforeEach(function () {
      var config = {
        hashKey: 'id',
        schema : {
          id    : Joi.string(),
          email : Joi.string(),
          age   : Joi.number(),
          names : Schema.types.stringSet()
        }
      };

      schema = new Schema(config);
    });

    it('should return single SET action', function () {
      var updates = {
        id : 'foobar',
        email : 'test@test.com',
      };

      var result = expressions.serializeUpdateExpression(schema, updates);

      expect(result.expressions).to.eql({
        SET    : ['#email = :email'],
        ADD    : [],
        REMOVE : [],
        DELETE : [],
      });

      expect(result.values).to.eql({ ':email' : 'test@test.com' });
      expect(result.attributeNames).to.eql({ '#email' : 'email' });
    });

    it('should return multiple SET actions', function () {
      var updates = {
        id : 'foobar',
        email : 'test@test.com',
        age : 33,
        name : 'Steve'
      };

      var result = expressions.serializeUpdateExpression(schema, updates);

      expect(result.expressions).to.eql({
        SET    : ['#email = :email', '#age = :age', '#name = :name'],
        ADD    : [],
        REMOVE : [],
        DELETE : [],
      });

      expect(result.values).to.eql({
        ':email' : 'test@test.com',
        ':age'   : 33,
        ':name'  : 'Steve'
      });

      expect(result.attributeNames).to.eql({
        '#email' : 'email',
        '#age'   : 'age',
        '#name'  : 'name',
      });
    });

    it('should return SET and ADD actions', function () {
      var updates ={
        id : 'foobar',
        email : 'test@test.com',
        age : {$add : 1}
      };

      var result = expressions.serializeUpdateExpression(schema, updates);
      expect(result.expressions).to.eql({
        SET    : ['#email = :email'],
        ADD    : ['#age :age'],
        REMOVE : [],
        DELETE : [],
      });

      expect(result.values).to.eql({
        ':email' : 'test@test.com',
        ':age'   : 1
      });

      expect(result.attributeNames).to.eql({
        '#email' : 'email',
        '#age'   : 'age',
      });

    });

    it('should return single DELETE action', function () {
      var updates = {
        id : 'foobar',
        names : { $del : 'tester'},
      };

      var result = expressions.serializeUpdateExpression(schema, updates);

      expect(result.expressions).to.eql({
        SET    : [],
        ADD    : [],
        REMOVE : [],
        DELETE : ['#names :names'],
      });

      var stringSet = result.values[':names'];

      expect(result.values).to.have.keys([':names']);
      expect(result.values[':names'].type).eql('String');
      expect(stringSet.values).to.eql([ 'tester']);
      expect(stringSet.type).to.eql( 'String');

      expect(result.attributeNames).to.eql({
        '#names' : 'names'
      });

    });

    it('should return single REMOVE action', function () {
      var updates = {
        id : 'foobar',
        email : null,
      };

      var result = expressions.serializeUpdateExpression(schema, updates);

      expect(result.expressions).to.eql({
        SET    : [],
        ADD    : [],
        REMOVE : ['#email'],
        DELETE : [],
      });

      expect(result.values).to.eql({});

      expect(result.attributeNames).to.eql({
        '#email' : 'email'
      });

    });

    it('should return single REMOVE action when value is set to empty string', function () {
      var updates = {
        id : 'foobar',
        email : '',
      };

      var result = expressions.serializeUpdateExpression(schema, updates);

      expect(result.expressions).to.eql({
        SET    : [],
        ADD    : [],
        REMOVE : ['#email'],
        DELETE : [],
      });

      expect(result.values).to.eql({});

      expect(result.attributeNames).to.eql({
        '#email' : 'email'
      });

    });

    it('should return empty actions when passed empty object', function () {
      var result = expressions.serializeUpdateExpression(schema, {});

      expect(result.expressions).to.eql({
        SET    : [],
        ADD    : [],
        REMOVE : [],
        DELETE : [],
      });

      expect(result.values).to.eql({});
      expect(result.attributeNames).to.eql({});
    });

    it('should return empty actions when passed null', function () {
      var result = expressions.serializeUpdateExpression(schema, null);

      expect(result.expressions).to.eql({
        SET    : [],
        ADD    : [],
        REMOVE : [],
        DELETE : [],
      });

      expect(result.values).to.eql({});
      expect(result.attributeNames).to.eql({});
    });

  });

  describe('#stringify', function () {

    it('should return single SET action', function () {
      var params = {
        SET : ['#email = :email']
      };

      var out = expressions.stringify(params);
      expect(out).to.eql('SET #email = :email');
    });

    it('should return single SET action when param is a string', function () {
      var params = {
        SET : '#email = :email'
      };

      var out = expressions.stringify(params);
      expect(out).to.eql('SET #email = :email');
    });

    it('should return single SET action when other actions are null', function () {
      var params = {
        SET    : ['#email = :email'],
        ADD    : null,
        REMOVE : null,
        DELETE : null
      };

      var out = expressions.stringify(params);
      expect(out).to.eql('SET #email = :email');
    });

    it('should return multiple SET actions', function () {
      var params = {
        SET : ['#email = :email', '#age = :n', '#name = :name']
      };

      var out = expressions.stringify(params);
      expect(out).to.eql('SET #email = :email, #age = :n, #name = :name');
    });

    it('should return SET and ADD actions', function () {
      var params = {
        SET : ['#email = :email'],
        ADD : ['#age :n', '#foo :bar']
      };

      var out = expressions.stringify(params);
      expect(out).to.eql('SET #email = :email ADD #age :n, #foo :bar');
    });

    it('should return stringified ALL actions', function () {
      var params = {
        SET : ['#email = :email'],
        ADD : ['#age :n', '#foo :bar'],
        REMOVE : ['#title', '#picture', '#settings'],
        DELETE : ['#color :c']
      };

      var out = expressions.stringify(params);
      expect(out).to.eql('SET #email = :email ADD #age :n, #foo :bar REMOVE #title, #picture, #settings DELETE #color :c');
    });

    it('should return empty string when passed empty actions', function () {
      var params = {
        SET : [],
        ADD : [],
        REMOVE : [],
        DELETE : []
      };

      var out = expressions.stringify(params);
      expect(out).to.eql('');
    });

    it('should return empty string when passed null actions', function () {
      var params = {
        SET : null,
        ADD : null,
        REMOVE : null,
        DELETE : null
      };

      var out = expressions.stringify(params);
      expect(out).to.eql('');
    });

    it('should return empty string when passed empty object', function () {
      var out = expressions.stringify({});

      expect(out).to.eql('');
    });

    it('should return empty string when passed null', function () {
      var out = expressions.stringify(null);

      expect(out).to.eql('');
    });

    it('should result from stringifying a parsed string should equal original string', function () {
      var exp = 'SET #email = :email ADD #age :n, #foo :bar REMOVE #title, #picture, #settings DELETE #color :c';
      var parsed = expressions.parse(exp);

      expect(expressions.stringify(parsed)).to.eql(exp);
    });

  });

});
