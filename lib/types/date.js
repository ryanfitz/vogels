'use strict';

// Load modules

var NodeUtil = require('util');
var Joi = require('joi');
var BaseType = Joi.types.Base;
var _ = require('lodash');


// Declare internals

var internals = {};


module.exports = internals.createType = function () {
  return new internals.DateType();
};

module.exports.DateType = internals.DateType = function () {

  /*jshint camelcase:false */
  internals.DateType.super_.call(this);
  _.extend(this, BaseType);
  return this;
};

NodeUtil.inherits(internals.DateType, BaseType);


internals.DateType.prototype.__name = 'Date';


//internals.DateType.prototype.convert = function (value) {

  //if (typeof value === 'string') {
    //return new Date(value);
  //}

  //return value;
//};

internals.DateType.prototype._base = function () {

  return function (value, obj, key, errors, keyPath) {

    errors = errors || {};
    errors.add = errors.add || function () { };
    value = (isNaN(Number(value)) === false) ? +value : value;
    var converted = new Date(value);

    var result = (!isNaN(converted.getTime()));
    if (!result) {
      errors.add('the value of ' + key + ' must be a valid JavaScript Date format', keyPath);
    }
    return result;
  };
};

internals.DateType.prototype.base = function () {

  this.add('base', this._base(), arguments);
  return this;
};
