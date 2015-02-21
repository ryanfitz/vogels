'use strict';

var _ = require('lodash'),
    utils = require('./utils'),
    serializer = require('./serializer');

var internals = {};

internals.actionWords = ['SET', 'ADD', 'REMOVE', 'DELETE'];

internals.regexMap = _.reduce(internals.actionWords, function (result, key) {
  result[key] = new RegExp(key + '\\s*(.+?)\\s*(SET|ADD|REMOVE|DELETE|$)');
  return result;
}, {});

// explanation http://stackoverflow.com/questions/3428618/regex-to-find-commas-that-arent-inside-and
internals.splitOperandsRegex = new RegExp(/\s*(?![^(]*\)),\s*/);

internals.match = function (actionWord, str) {
  var match = internals.regexMap[actionWord].exec(str);

  if(match && match.length >= 2) {
    return match[1].split(internals.splitOperandsRegex);
  } else {
    return null;
  }
};

exports.parse = function (str) {
  return _.reduce(internals.actionWords, function (result, actionWord) {
    result[actionWord] = internals.match(actionWord, str);
    return result;
  }, {});
};

exports.serializeUpdateExpression = function (schema, item) {
  var datatypes = schema._modelDatatypes;

  var data = utils.omitPrimaryKeys(schema, item);

  var memo = {
    expressions : {},
    attributeNames : {},
    values : {},
  };

  memo.expressions = _.reduce(internals.actionWords, function (result, key) {
    result[key] = [];

    return result;
  }, {});

  var result = _.reduce(data, function (result, value, key) {
    var valueKey = ':' + key;
    var nameKey = '#' + key;

    if(_.isNull(value)) {
      result.expressions.REMOVE.push(nameKey);
      result.attributeNames[nameKey] = key;
    } else if (_.isPlainObject(value) && value.$add) {
      result.expressions.ADD.push(nameKey + ' ' + valueKey);
      result.values[valueKey] = serializer.serializeAttribute(value.$add, datatypes[key]);
      result.attributeNames[nameKey] = key;
    } else if (_.isPlainObject(value) && value.$del) {
      result.expressions.DELETE.push(nameKey + ' ' + valueKey);
      result.values[valueKey] = serializer.serializeAttribute(value.$del, datatypes[key]);
      result.attributeNames[nameKey] = key;
    } else {
      result.expressions.SET.push(nameKey + ' = ' + valueKey);
      result.values[valueKey] = serializer.serializeAttribute(value, datatypes[key]);
      result.attributeNames[nameKey] = key;
    }

    return result;
  }, memo);

  return result;
};

exports.stringify = function (expressions) {
  return _.reduce(expressions, function (result, value, key) {
    if(!_.isEmpty(value)) {
      if(_.isArray(value)) {
        result.push(key + ' ' + value.join(', '));
      } else {
        result.push(key + ' ' + value);
      }
    }

    return result;
  }, []).join(' ');
};

