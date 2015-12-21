'use strict';

var vogels = require('../../index'),
    chai   = require('chai'),
    expect = chai.expect,
    //async  = require('async'),
    _      = require('lodash'),
    helper = require('../test-helper'),
    Joi    = require('joi');

chai.should();

describe('Create Tables Integration Tests', function() {
  this.timeout(0);

  before(function () {
    vogels.dynamoDriver(helper.realDynamoDB());
  });

  afterEach(function () {
    vogels.reset();
  });

  it('should create table with hash key', function (done) {
    var Model = vogels.define('vogels-create-table-test', {
      hashKey : 'id',
      tableName : helper.randomName('vogels-createtable-Accounts'),
      schema : {
        id : Joi.string(),
      }
    });

    Model.createTable(function (err, result) {
      expect(err).to.not.exist;

      var desc = result.TableDescription;
      expect(desc).to.exist;
      expect(desc.KeySchema).to.eql([{ AttributeName: 'id', KeyType: 'HASH' } ]);

      expect(desc.AttributeDefinitions).to.eql([
        { AttributeName: 'id', AttributeType: 'S' },
      ]);

      return Model.deleteTable(done);
    });
  });

  it('should create table with hash and range key', function (done) {
    var Model = vogels.define('vogels-createtable-rangekey', {
      hashKey : 'name',
      rangeKey : 'age',
      tableName : helper.randomName('vogels-createtable-rangekey'),
      schema : {
        name : Joi.string(),
        age : Joi.number(),
      }
    });

    Model.createTable(function (err, result) {
      expect(err).to.not.exist;

      var desc = result.TableDescription;

      expect(desc).to.exist;

      expect(desc.AttributeDefinitions).to.eql([
        { AttributeName: 'name', AttributeType: 'S' },
        { AttributeName: 'age', AttributeType: 'N' }
      ]);

      expect(desc.KeySchema).to.eql([
        { AttributeName: 'name', KeyType: 'HASH' },
        { AttributeName: 'age', KeyType: 'RANGE' }
      ]);

      return Model.deleteTable(done);
    });
  });

  it('should create table with local secondary index', function (done) {
    var Model = vogels.define('vogels-createtable-rangekey', {
      hashKey : 'name',
      rangeKey : 'age',
      tableName : helper.randomName('vogels-createtable-local-idx'),
      schema : {
        name : Joi.string(),
        age : Joi.number(),
        nick : Joi.string(),
        time : Joi.date()
      },
      indexes : [
        {hashKey : 'name', rangeKey : 'nick', type : 'local', name : 'NickIndex'},
        {hashKey : 'name', rangeKey : 'time', type : 'local', name : 'TimeIndex'},
      ]
    });

    Model.createTable(function (err, result) {
      expect(err).to.not.exist;

      var desc = result.TableDescription;

      expect(desc).to.exist;

      expect(desc.AttributeDefinitions).to.eql([
        { AttributeName: 'name', AttributeType: 'S' },
        { AttributeName: 'age', AttributeType: 'N' },
        { AttributeName: 'nick', AttributeType: 'S' },
        { AttributeName: 'time', AttributeType: 'S' }
      ]);

      expect(desc.KeySchema).to.eql([
        { AttributeName: 'name', KeyType: 'HASH' },
        { AttributeName: 'age', KeyType: 'RANGE' }
      ]);

      expect(desc.LocalSecondaryIndexes).to.have.length(2);

      var nickIndex = _.find(desc.LocalSecondaryIndexes, { IndexName: 'NickIndex' });
      expect(nickIndex.IndexName).to.eql('NickIndex');
      expect(nickIndex.Projection).to.eql({ ProjectionType: 'ALL' });
      expect(nickIndex.KeySchema).to.eql([
        { AttributeName: 'name', KeyType: 'HASH' },
        { AttributeName: 'nick', KeyType: 'RANGE' },
      ]);

      var timeIndex = _.find(desc.LocalSecondaryIndexes, { IndexName: 'TimeIndex' });
      expect(timeIndex.IndexName).to.eql('TimeIndex');
      expect(timeIndex.Projection).to.eql({ ProjectionType: 'ALL' });
      expect(timeIndex.KeySchema).to.eql([
        { AttributeName: 'name', KeyType: 'HASH' },
        { AttributeName: 'time', KeyType: 'RANGE' },
      ]);

      return Model.deleteTable(done);
    });
  });

  it('should create table with local secondary index with custom projection', function (done) {
    var Model = vogels.define('vogels-createtable-local-proj', {
      hashKey : 'name',
      rangeKey : 'age',
      tableName : helper.randomName('vogels-createtable-local-proj'),
      schema : {
        name : Joi.string(),
        age : Joi.number(),
        nick : Joi.string()
      },
      indexes : [{
        hashKey : 'name',
        rangeKey : 'nick',
        type : 'local',
        name : 'KeysOnlyNickIndex',
        projection : { ProjectionType: 'KEYS_ONLY'}
      }]
    });

    Model.createTable(function (err, result) {
      expect(err).to.not.exist;

      var desc = result.TableDescription;

      expect(desc).to.exist;

      expect(desc.AttributeDefinitions).to.eql([
        { AttributeName: 'name', AttributeType: 'S' },
        { AttributeName: 'age', AttributeType: 'N' },
        { AttributeName: 'nick', AttributeType: 'S' }
      ]);

      expect(desc.KeySchema).to.eql([
        { AttributeName: 'name', KeyType: 'HASH' },
        { AttributeName: 'age', KeyType: 'RANGE' }
      ]);

      var nickIndex = _.find(desc.LocalSecondaryIndexes, { IndexName: 'KeysOnlyNickIndex' });
      expect(nickIndex.IndexName).to.eql('KeysOnlyNickIndex');
      expect(nickIndex.Projection).to.eql({ ProjectionType: 'KEYS_ONLY' });
      expect(nickIndex.KeySchema).to.eql([
        { AttributeName: 'name', KeyType: 'HASH' },
        { AttributeName: 'nick', KeyType: 'RANGE' },
      ]);

      return Model.deleteTable(done);
    });
  });

  it('should create table with global index', function (done) {
    var Model = vogels.define('vogels-createtable-global', {
      hashKey : 'name',
      rangeKey : 'age',
      tableName : helper.randomName('vogels-createtable-global'),
      schema : {
        name : Joi.string(),
        age : Joi.number(),
        nick : Joi.string()
      },
      indexes : [{hashKey : 'nick', type : 'global', name : 'GlobalNickIndex'}]
    });

    Model.createTable(function (err, result) {
      expect(err).to.not.exist;

      var desc = result.TableDescription;

      expect(desc).to.exist;

      expect(desc.AttributeDefinitions).to.eql([
        { AttributeName: 'name', AttributeType: 'S' },
        { AttributeName: 'age', AttributeType: 'N' },
        { AttributeName: 'nick', AttributeType: 'S' }
      ]);

      expect(desc.KeySchema).to.eql([
        { AttributeName: 'name', KeyType: 'HASH' },
        { AttributeName: 'age', KeyType: 'RANGE' }
      ]);

      var nickIndex = _.find(desc.GlobalSecondaryIndexes, { IndexName: 'GlobalNickIndex' });
      expect(nickIndex.IndexName).to.eql('GlobalNickIndex');
      expect(nickIndex.Projection).to.eql({ ProjectionType: 'ALL' });
      expect(nickIndex.KeySchema).to.eql([
        { AttributeName: 'nick', KeyType: 'HASH' },
      ]);
      expect(nickIndex.ProvisionedThroughput).to.eql({ ReadCapacityUnits : 1, WriteCapacityUnits : 1});

      return Model.deleteTable(done);
    });
  });

  it('should create table with global index with optional settings', function (done) {
    var Model = vogels.define('vogels-createtable-global', {
      hashKey : 'name',
      rangeKey : 'age',
      tableName : helper.randomName('vogels-createtable-global'),
      schema : {
        name : Joi.string(),
        age  : Joi.number(),
        nick : Joi.string(),
        wins : Joi.number()
      },
      indexes : [{
        hashKey       : 'nick',
        type          : 'global',
        name          : 'GlobalNickIndex',
        projection    : { NonKeyAttributes : [ 'wins' ], ProjectionType : 'INCLUDE' },
        readCapacity  : 10,
        writeCapacity : 5
      }]
    });

    Model.createTable(function (err, result) {
      expect(err).to.not.exist;

      var desc = result.TableDescription;

      expect(desc).to.exist;

      expect(desc.AttributeDefinitions).to.eql([
        { AttributeName: 'name', AttributeType: 'S' },
        { AttributeName: 'age', AttributeType: 'N' },
        { AttributeName: 'nick', AttributeType: 'S' }
      ]);

      expect(desc.KeySchema).to.eql([
        { AttributeName: 'name', KeyType: 'HASH' },
        { AttributeName: 'age', KeyType: 'RANGE' }
      ]);

      var nickIndex = _.find(desc.GlobalSecondaryIndexes, { IndexName: 'GlobalNickIndex' });
      expect(nickIndex.IndexName).to.eql('GlobalNickIndex');
      expect(nickIndex.Projection).to.eql({ ProjectionType: 'INCLUDE', NonKeyAttributes : [ 'wins' ] });
      expect(nickIndex.KeySchema).to.eql([
        { AttributeName: 'nick', KeyType: 'HASH' },
      ]);
      expect(nickIndex.ProvisionedThroughput).to.eql({ ReadCapacityUnits : 10, WriteCapacityUnits : 5});

      return Model.deleteTable(done);
    });
  });

  it('should create table with global and local indexes', function (done) {
    var Model = vogels.define('vogels-createtable-both-indexes', {
      hashKey : 'name',
      rangeKey : 'age',
      tableName : helper.randomName('vogels-createtable-both-indexes'),
      schema : {
        name : Joi.string(),
        age  : Joi.number(),
        nick : Joi.string(),
        wins : Joi.number()
      },
      indexes : [
        { hashKey : 'name', rangeKey : 'nick', type   : 'local', name        : 'NameNickIndex'},
        { hashKey : 'name', rangeKey : 'wins', type   : 'local', name        : 'NameWinsIndex'},
        { hashKey : 'nick', type     : 'global', name : 'GlobalNickIndex' },
        { hashKey : 'age' , rangeKey : 'wins', type   : 'global', name       : 'GlobalAgeWinsIndex' }
      ]
    });

    Model.createTable(function (err, result) {
      expect(err).to.not.exist;

      var desc = result.TableDescription;

      expect(desc).to.exist;

      expect(desc.AttributeDefinitions).to.eql([
        { AttributeName: 'name', AttributeType: 'S' },
        { AttributeName: 'age', AttributeType: 'N' },
        { AttributeName: 'nick', AttributeType: 'S' },
        { AttributeName: 'wins', AttributeType: 'N' }
      ]);

      expect(desc.KeySchema).to.eql([
        { AttributeName: 'name', KeyType: 'HASH' },
        { AttributeName: 'age', KeyType: 'RANGE' }
      ]);

      expect(desc.GlobalSecondaryIndexes).to.have.length(2);

      var nickIndex = _.find(desc.GlobalSecondaryIndexes, { IndexName: 'GlobalNickIndex' });
      expect(nickIndex.IndexName).to.eql('GlobalNickIndex');
      expect(nickIndex.Projection).to.eql({ ProjectionType: 'ALL' });
      expect(nickIndex.KeySchema).to.eql([
        { AttributeName: 'nick', KeyType: 'HASH' },
      ]);
      expect(nickIndex.ProvisionedThroughput).to.eql({ ReadCapacityUnits : 1, WriteCapacityUnits : 1});

      var ageWinsIndex = _.find(desc.GlobalSecondaryIndexes, { IndexName: 'GlobalAgeWinsIndex' });
      expect(ageWinsIndex.IndexName).to.eql('GlobalAgeWinsIndex');
      expect(ageWinsIndex.Projection).to.eql({ ProjectionType: 'ALL' });
      expect(ageWinsIndex.KeySchema).to.eql([
        { AttributeName: 'age', KeyType: 'HASH' },
        { AttributeName: 'wins', KeyType: 'RANGE' },
      ]);
      expect(ageWinsIndex.ProvisionedThroughput).to.eql({ ReadCapacityUnits : 1, WriteCapacityUnits : 1});

      expect(desc.LocalSecondaryIndexes).to.have.length(2);

      var nameNickIndex = _.find(desc.LocalSecondaryIndexes, { IndexName: 'NameNickIndex' });
      expect(nameNickIndex.IndexName).to.eql('NameNickIndex');
      expect(nameNickIndex.Projection).to.eql({ ProjectionType: 'ALL' });
      expect(nameNickIndex.KeySchema).to.eql([
        { AttributeName: 'name', KeyType: 'HASH' },
        { AttributeName: 'nick', KeyType: 'RANGE' },
      ]);

      var nameWinsIndex = _.find(desc.LocalSecondaryIndexes, { IndexName: 'NameWinsIndex' });
      expect(nameWinsIndex.IndexName).to.eql('NameWinsIndex');
      expect(nameWinsIndex.Projection).to.eql({ ProjectionType: 'ALL' });
      expect(nameWinsIndex.KeySchema).to.eql([
        { AttributeName: 'name', KeyType: 'HASH' },
        { AttributeName: 'wins', KeyType: 'RANGE' },
      ]);

      return Model.deleteTable(done);
    });
  });
});

describe('Update Tables Integration Tests', function() {
  this.timeout(0);
  var Tweet,
      tableName;

  before(function (done) {
    vogels.dynamoDriver(helper.realDynamoDB());

    tableName = helper.randomName('vogels-updateTable-Tweets');

    Tweet = vogels.define('vogels-update-table-test', {
      hashKey  : 'UserId',
      rangeKey : 'TweetID',
      tableName : tableName,
      schema : {
        UserId            : Joi.string(),
        TweetID           : vogels.types.uuid(),
        content           : Joi.string(),
        PublishedDateTime : Joi.date().default(Date.now)
      }
    });

    vogels.createTables(done);
  });

  afterEach(function () {
    vogels.reset();
  });

  it('should add global secondary index', function (done) {
    Tweet = vogels.define('vogels-update-table-test', {
      hashKey  : 'UserId',
      rangeKey : 'TweetID',
      tableName : tableName,
      schema : {
        UserId            : Joi.string(),
        TweetID           : vogels.types.uuid(),
        content           : Joi.string(),
        PublishedDateTime : Joi.date().default(Date.now)
      },
      indexes : [
        { hashKey : 'UserId', rangeKey : 'PublishedDateTime', type : 'global', name : 'PublishedDateTimeIndex'}
      ]
    });

    Tweet.updateTable(function (err) {
      expect(err).to.not.exist;

      Tweet.describeTable(function (err, data) {
        expect(err).to.not.exist;

        var globalIndexes = _.get(data, 'Table.GlobalSecondaryIndexes');
        expect(globalIndexes).to.have.length(1);

        var idx = _.first(globalIndexes);
        expect(idx.IndexName).to.eql('PublishedDateTimeIndex');
        expect(idx.KeySchema).to.eql([{AttributeName: 'UserId', KeyType: 'HASH'},
                                      {AttributeName: 'PublishedDateTime', KeyType:'RANGE'}]);
        expect(idx.Projection).to.eql({ProjectionType : 'ALL'});

        return done();
      });
    });

  });

});
