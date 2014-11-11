'use strict';

var sinon = require('sinon'),
    AWS   = require('aws-sdk'),
    Table = require('../lib/table'),
    DOC   = require('dynamodb-doc'),
    _     = require('lodash');

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
  return new DOC.DynamoDB(exports.mockDynamoDB());
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
