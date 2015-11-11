'use strict';

var sinon  = require('sinon'),
    AWS    = require('aws-sdk'),
    Table  = require('../lib/table'),
    _      = require('lodash'),
    bunyan = require('bunyan');

exports.mockDynamoDB = function () {
  var opts = { endpoint : 'http://dynamodb-local:8000', apiVersion: '2012-08-10' };
  var db = new AWS.DynamoDB(opts);

  db.scan          = sinon.stub();
  db.putItem       = sinon.stub();
  db.deleteItem    = sinon.stub();
  db.query         = sinon.stub();
  db.getItem       = sinon.stub();
  db.updateItem    = sinon.stub();
  db.createTable   = sinon.stub();
  db.describeTable = sinon.stub();
  db.updateTable   = sinon.stub();
  db.deleteTable   = sinon.stub();
  db.batchGetItem  = sinon.stub();
  db.batchWriteItem = sinon.stub();

  return db;
};

exports.realDynamoDB = function () {
  var opts = { endpoint : 'http://dynamodb-local:8000', apiVersion: '2012-08-10' };
  return new AWS.DynamoDB(opts);
};

exports.mockDocClient = function () {
  var client = new AWS.DynamoDB.DocumentClient({service : exports.mockDynamoDB()});

  var operations= [
    'batchGet',
    'batchWrite',
    'put',
    'get',
    'delete',
    'update',
    'scan',
    'query'
  ];

  _.each(operations, function (op) {
    client[op] = sinon.stub();
  });

  client.service.scan          = sinon.stub();
  client.service.putItem       = sinon.stub();
  client.service.deleteItem    = sinon.stub();
  client.service.query         = sinon.stub();
  client.service.getItem       = sinon.stub();
  client.service.updateItem    = sinon.stub();
  client.service.createTable   = sinon.stub();
  client.service.describeTable = sinon.stub();
  client.service.updateTable   = sinon.stub();
  client.service.deleteTable   = sinon.stub();
  client.service.batchGetItem  = sinon.stub();
  client.service.batchWriteItem = sinon.stub();

  return client;
};

exports.mockSerializer = function () {
  var serializer = {
    buildKey               : sinon.stub(),
    deserializeItem        : sinon.stub(),
    serializeItem          : sinon.stub(),
    serializeItemForUpdate : sinon.stub()
  };

  return serializer;
};

exports.mockTable = function () {
  return sinon.createStubInstance(Table);
};

exports.fakeUUID = function () {
  var uuid = {
    v1: sinon.stub(),
    v4: sinon.stub()
  };

  return uuid;
};

exports.randomName = function (prefix) {
  return prefix + '_' + Date.now() + '.' + _.random(1000);
};

exports.testLogger = function() {
  return bunyan.createLogger({
    name: 'vogels-tests',
    serializers : {err: bunyan.stdSerializers.err},
    level : bunyan.FATAL
  });
};
