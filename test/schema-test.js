'use strict';

var Schema = require('../lib/schema'),
    chai   = require('chai'),
    expect = chai.expect,
    Joi    = require('joi'),
    _      = require('lodash'),
    sinon  = require('sinon');

chai.should();

describe('schema', function () {

  describe('setup', function () {

    it('should set hash key', function () {
      var config = {
        hashKey: 'id'
      };

      var s = new Schema(config);
      s.hashKey.should.equal('id');
    });

    it('should set hash and range key', function () {
      var config = {
        hashKey : 'id',
        rangeKey : 'date'
      };

      var s = new Schema(config);
      s.hashKey.should.equal('id');
      s.rangeKey.should.equal('date');
    });

    it('should set table name to string', function () {
      var config = {
        hashKey : 'id',
        tableName : 'test-table'
      };

      var s = new Schema(config);
      s.tableName.should.equal('test-table');
    });

    it('should set table name to function', function () {
      var func = function () { return 'test-table'; };

      var config = {
        hashKey : 'id',
        tableName : func
      };

      var s = new Schema(config);
      s.tableName.should.equal(func);
    });

    it('should add timestamps to schema', function () {
      var config = {
        hashKey : 'id',
        timestamps : true,
        schema : {
          id : Joi.string()
        }
      };

      var s = new Schema(config);
      s.timestamps.should.be.true;

      expect(s._modelSchema.describe().children).to.have.keys(['id', 'createdAt', 'updatedAt']);

      s._modelDatatypes.should.eql({
        id  : 'S',
        createdAt : 'DATE',
        updatedAt : 'DATE',
      });
    });

    it('should add timestamps with custom names to schema', function () {
      var config = {
        hashKey : 'id',
        timestamps : true,
        createdAt : 'created',
        updatedAt : 'updated',
        schema : {
          id : Joi.string()
        }
      };

      var s = new Schema(config);
      s.timestamps.should.be.true;

      expect(s._modelSchema.describe().children).to.have.keys(['id', 'created', 'updated']);

      s._modelDatatypes.should.eql({
        id  : 'S',
        created : 'DATE',
        updated : 'DATE',
      });
    });

    it('should only add createdAt timestamp ', function () {
      var config = {
        hashKey : 'id',
        timestamps : true,
        updatedAt : false,
        schema : {
          id : Joi.string()
        }
      };

      var s = new Schema(config);
      s.timestamps.should.be.true;

      expect(s._modelSchema.describe().children).to.have.keys(['id', 'createdAt']);

      s._modelDatatypes.should.eql({
        id  : 'S',
        createdAt : 'DATE'
      });
    });

    it('should only add updatedAt timestamp ', function () {
      var config = {
        hashKey : 'id',
        timestamps : true,
        createdAt : false,
        schema : {
          id : Joi.string()
        }
      };

      var s = new Schema(config);
      s.timestamps.should.be.true;

      expect(s._modelSchema.describe().children).to.have.keys(['id', 'updatedAt']);

      s._modelDatatypes.should.eql({
        id  : 'S',
        updatedAt : 'DATE'
      });
    });

    it('should only add custom created timestamp ', function () {
      var config = {
        hashKey : 'id',
        timestamps : true,
        createdAt : 'fooCreate',
        updatedAt : false,
        schema : {
          id : Joi.string()
        }
      };

      var s = new Schema(config);
      s.timestamps.should.be.true;

      expect(s._modelSchema.describe().children).to.have.keys(['id', 'fooCreate']);

      s._modelDatatypes.should.eql({
        id  : 'S',
        fooCreate : 'DATE'
      });
    });

    it('should throw error when hash key is not present', function () {
      var config = {rangeKey : 'foo'};

      expect(function () {
        new Schema(config);
      }).to.throw(/hashKey is required/);

    });

    it('should setup local secondary index when both hash and range keys are given', function () {
      var config = {
        hashKey : 'foo',
        indexes : [
          {hashKey : 'foo', rangeKey : 'bar', type:'local', name : 'LocalBarIndex'}
        ]
      };

      var s = new Schema(config);
      s.secondaryIndexes.should.include.keys('LocalBarIndex');
      s.globalIndexes.should.be.empty;
    });

    it('should setup local secondary index when only range key is given', function () {
      var config = {
        hashKey : 'foo',
        indexes : [
          {rangeKey : 'bar', type:'local', name : 'LocalBarIndex'}
        ]
      };

      var s = new Schema(config);
      s.secondaryIndexes.should.include.keys('LocalBarIndex');
      s.globalIndexes.should.be.empty;
    });

    it('should throw when local index rangeKey isnt present', function () {
      var config = {
        hashKey : 'foo',
        indexes : [
          {hashKey : 'foo', type:'local', name : 'LocalBarIndex'}
        ]
      };

      expect(function () {
        new Schema(config);
      }).to.throw(/rangeKey.*missing/);
    });

    it('should throw when local index hashKey does not match the tables hashKey', function () {
      var config = {
        hashKey : 'foo',
        indexes : [
          {hashKey : 'bar', rangeKey: 'date', type:'local', name : 'LocalDateIndex'}
        ]
      };

      expect(function () {
        new Schema(config);
      }).to.throw(/hashKey must be one of context:hashKey/);
    });

    it('should setup global index', function () {
      var config = {
        hashKey : 'foo',
        indexes : [
          {hashKey : 'bar', type:'global', name : 'GlobalBarIndex'}
        ]
      };

      var s = new Schema(config);
      s.globalIndexes.should.include.keys('GlobalBarIndex');
      s.secondaryIndexes.should.be.empty;
    });

    it('should throw when global index hashKey is not present', function () {
      var config = {
        hashKey : 'foo',
        indexes : [
          {rangeKey: 'date', type:'global', name : 'GlobalDateIndex'}
        ]
      };

      expect(function () {
        new Schema(config);
      }).to.throw(/hashKey is required/);
    });

    it('should parse schema data types', function () {
      var config = {
        hashKey : 'foo',
        schema : Joi.object().keys({
          foo  : Joi.string().default('foobar'),
          date : Joi.date().default(Date.now),
          count: Joi.number(),
          flag: Joi.boolean(),
          nums : Joi.array().includes(Joi.number()).meta({dynamoType : 'NS'}),
          items : Joi.array(),
          data : Joi.object().keys({
            stuff : Joi.array().meta({dynamoType : 'SS'}),
            nested : {
              first : Joi.string(),
              last : Joi.string(),
              nicks : Joi.array().meta({dynamoType : 'SS', foo : 'bar'}),
              ages : Joi.array().meta({foo : 'bar'}).meta({dynamoType : 'NS'}),
              pics : Joi.array().meta({dynamoType : 'BS'}),
              bin : Joi.binary()
            }
          })
        })
      };

      var s = new Schema(config);

      s._modelSchema.should.eql(config.schema);
      s._modelDatatypes.should.eql({
        foo  : 'S',
        date : 'DATE',
        count : 'N',
        flag : 'BOOL',
        nums : 'NS',
        items : 'L',
        data : {
          nested : {
            ages  : 'NS',
            first : 'S',
            last  : 'S',
            nicks : 'SS',
            pics  : 'BS',
            bin   : 'B'
          },
          stuff : 'SS'
        }
      });
    });

  });

  describe('#stringSet', function () {
    it('should set as string set', function () {
      var config = {
        hashKey : 'email',
        schema : {
          email : Joi.string().email(),
          names : Schema.types.stringSet()
        }
      };

      var s = new Schema(config);

      s._modelDatatypes.should.eql({
        email  : 'S',
        names : 'SS',
      });
    });
  });

  describe('#numberSet', function () {
    it('should set as number set', function () {
      var config = {
        hashKey : 'email',
        schema : {
          email : Joi.string().email(),
          nums : Schema.types.numberSet()
        }
      };

      var s = new Schema(config);

      s._modelDatatypes.should.eql({
        email  : 'S',
        nums : 'NS',
      });
    });
  });

  describe('#binarySet', function () {
    it('should set as binary set', function () {
      var config = {
        hashKey : 'email',
        schema : {
          email : Joi.string().email(),
          pics : Schema.types.binarySet()
        }
      };

      var s = new Schema(config);

      s._modelDatatypes.should.eql({
        email  : 'S',
        pics : 'BS',
      });
    });
  });


  describe('#uuid', function () {
    it('should set as uuid with default uuid function', function () {
      var config = {
        hashKey : 'id',
        schema : {
          id : Schema.types.uuid(),
        }
      };

      var s = new Schema(config);
      expect(s.applyDefaults({}).id).should.not.be.empty;
    });
  });

  describe('#timeUUID', function () {
    it('should set as TimeUUID with default v1 uuid function', function () {
      var config = {
        hashKey : 'id',
        schema : {
          id : Schema.types.timeUUID(),
        }
      };

      var s = new Schema(config);
      expect(s.applyDefaults({}).id).should.not.be.empty;

    });

  });

  describe('#validate', function () {

    it('should return no err for string', function() {
      var config = {
        hashKey : 'email',
        schema : {
          email : Joi.string().email().required()
        }
      };

      var s = new Schema(config);

      expect(s.validate({email: 'foo@bar.com'}).error).to.be.null;
    });

    it('should return no error for valid date object', function() {
      var config = {
        hashKey : 'created',
        schema : {
          created : Joi.date()
        }
      };

      var s = new Schema(config);

      expect(s.validate({created: new Date()}).error).to.be.null;
      expect(s.validate({created: Date.now()}).error).to.be.null;
    });

  });

  describe('#applyDefaults', function () {
    it('should apply default values', function () {
      var config = {
        hashKey : 'email',
        schema : {
          email : Joi.string(),
          name  : Joi.string().default('Foo Bar').required(),
          age   : Joi.number().default(3)
        }
      };

      var s = new Schema(config);

      var d = s.applyDefaults({email: 'foo@bar.com'});

      d.email.should.equal('foo@bar.com');
      d.name.should.equal('Foo Bar');
      d.age.should.equal(3);
    });

    it('should return result of default functions', function () {
      var clock = sinon.useFakeTimers(Date.now());

      var config = {
        hashKey : 'email',
        schema : {
          email   : Joi.string(),
          created : Joi.date().default(Date.now),
          data : {
            name : Joi.string().default('Tim Tester'),
            nick : Joi.string().default(_.constant('foo bar'))
          }
        }
      };

      var s = new Schema(config);

      var d = s.applyDefaults({email: 'foo@bar.com', data : {} });

      d.should.eql({
        email : 'foo@bar.com',
        created : Date.now(),
        data : {
          name : 'Tim Tester',
          nick : 'foo bar'
        }
      });

      clock.restore();
    });

  });

});
