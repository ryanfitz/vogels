'use strict';

//var internals = {};

var Table = module.exports = function (name, schema) {
  this.config = {name : name};
  this.scheme = schema;
};

Table.prototype.get = function () {
  throw new Error('not yet implemented');
};
