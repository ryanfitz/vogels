'use strict';

var sinon = require('sinon'),
    Table = require('../lib/table');

exports.mockDynamoDB = function () {
  var dynamodb = {
    scan        : sinon.stub(),
    putItem     : sinon.stub(),
    deleteItem  : sinon.stub(),
    query       : sinon.stub(),
    getItem     : sinon.stub(),
    updateItem  : sinon.stub(),
    createTable : sinon.stub(),
    describeTable : sinon.stub(),
    updateTable : sinon.stub()
  };

  return dynamodb;
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
