'use strict';

var _            = require('lodash'),
    util         = require('util'),
    AWS          = require('aws-sdk'),
    Table        = require('./table'),
    Schema       = require('./schema'),
    serializer   = require('./serializer'),
    batch        = require('./batch'),
    Item         = require('./item'),
    createTables = require('./createTables');

var vogels = module.exports;

vogels.AWS = AWS;

var internals = {};

vogels.dynamoDriver = internals.dynamoDriver = function (driver) {
  if(driver) {
    internals.dynamodb = driver;
    internals.updateDynamoDBDriverForAllModels(driver);
  } else {
    internals.dynamodb = internals.dynamodb || new vogels.AWS.DynamoDB({apiVersion: '2012-08-10'});
  }

  return internals.dynamodb;
};

internals.updateDynamoDBDriverForAllModels = function (driver) {
  _.each(vogels.models, function (model) {
    model.config({dynamodb: driver});
  });
};

internals.compileModel = function (name, schema) {

  // extremly simple table names
  var tableName = name.toLowerCase() + 's';

  var table = new Table(tableName, schema, serializer, internals.dynamoDriver());

  var Model = function (attrs) {
    Item.call(this, attrs, table);
  };

  util.inherits(Model, Item);

  Model.get          = _.bind(table.get, table);
  Model.create       = _.bind(table.create, table);
  Model.update       = _.bind(table.update, table);
  Model.destroy      = _.bind(table.destroy, table);
  Model.query        = _.bind(table.query, table);
  Model.scan         = _.bind(table.scan, table);
  Model.parallelScan = _.bind(table.parallelScan, table);

  Model.getItems = batch(table, serializer).getItems;
  Model.batchGetItems = batch(table, serializer).getItems;

  // table ddl methods
  Model.createTable   = _.bind(table.createTable, table);
  Model.updateTable   = _.bind(table.updateTable, table);
  Model.describeTable = _.bind(table.describeTable, table);
  Model.tableName     = _.bind(table.tableName, table);

  table.itemFactory = Model;

  // hooks
  Model.after  = _.bind(table.after, table);
  Model.before = _.bind(table.before, table);

  /* jshint camelcase:false */
  Model.__defineGetter__('dynamodb', function(){
    return table.dynamodb;
  });

  Model.config = function(config) {
    config = config || {};

    if(config.tableName) {
      table.config.name = config.tableName;
    }

    if(config.dynamodb) {
      table.dynamodb = config.dynamodb;
    }

    return table.config;
  };

  return vogels.model(name, Model);
};

internals.addModel = function (name, model) {
  vogels.models[name] = model;

  return vogels.models[name];
};

vogels.reset = function () {
  vogels.models = {};
};

vogels.define = function (modelName, callback) {
  var schema = new Schema();

  var compiledTable = internals.compileModel(modelName, schema);

  if(callback) {
    callback(schema);
  }

  return compiledTable;
};

vogels.model = function(name, model) {
  if(model) {
    internals.addModel(name, model);
  }

  return vogels.models[name] || null;
};

vogels.createTables = function (options, callback) {
  if (typeof options === 'function' && !callback) {
    callback = options;
    options = {};
  }

  callback = callback || function () {};

  return createTables(vogels.models, options, callback);
};

vogels.reset();
