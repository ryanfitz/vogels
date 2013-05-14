'use strict';

var _      = require('lodash'),
    util   = require('util'),
    Table  = require('./table'),
    Schema = require('./schema'),
    Item   = require('./item');

var vogels = module.exports;

var internals = {};

internals.compileModel = function (name, schema) {
  // extremly simple table names
  var tableName = name.toLowerCase() + 's';

  var table = new Table(tableName, schema);

  var Model = function (attrs) {
    Item.call(this, attrs, table);
  };

  util.inherits(Model, Item);

  Model.table = table;
  Model.get = _.bind(table.get, table);

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

vogels.reset();
