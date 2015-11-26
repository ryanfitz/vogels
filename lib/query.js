'use strict';

var _           = require('lodash'),
    expressions = require('./expressions'),
    utils       = require('./utils');

var internals = {};

internals.keyCondition = function (keyName, schema, query) {

  var f = function (operator) {
    return function (/*values*/) {
      var copy = [].slice.call(arguments);
      var existingValueKeys = _.keys(query.request.ExpressionAttributeValues);
      var args = [keyName, operator, existingValueKeys].concat(copy);
      var cond = expressions.buildFilterExpression.apply(null, args);
      return query.addKeyCondition(cond);
    };
  };

  return {
    equals      : f('='),
    eq          : f('='),
    lte         : f('<='),
    lt          : f('<'),
    gte         : f('>='),
    gt          : f('>'),
    beginsWith  : f('begins_with'),
    between     : f('BETWEEN')
  };
};

internals.queryFilter = function (keyName, schema, query) {

  var f = function (operator) {
    return function (/*values*/) {
      var copy = [].slice.call(arguments);
      var existingValueKeys = _.keys(query.request.ExpressionAttributeValues);
      var args = [keyName, operator, existingValueKeys].concat(copy);
      var cond = expressions.buildFilterExpression.apply(null, args);
      return query.addFilterCondition(cond);
    };
  };

  return {
    equals      : f('='),
    eq          : f('='),
    ne          : f('<>'),
    lte         : f('<='),
    lt          : f('<'),
    gte         : f('>='),
    gt          : f('>'),
    null        : f('attribute_not_exists'),
    exists      : f('attribute_exists'),
    contains    : f('contains'),
    notContains : f('NOT contains'),
    in          : f('IN'),
    beginsWith  : f('begins_with'),
    between     : f('BETWEEN')
  };
};

internals.isUsingGlobalIndex = function (query) {
  return query.request.IndexName && query.table.schema.globalIndexes[query.request.IndexName];
};

var Query = module.exports = function (hashKey, table, serializer) {
  this.hashKey = hashKey;
  this.table = table;
  this.serializer = serializer;

  this.options = {loadAll: false};
  this.request = {};
};

Query.prototype.limit = function(num) {
  if(num <= 0 ) {
    throw new Error('Limit must be greater than 0');
  }

  this.request.Limit = num;

  return this;
};

Query.prototype.filterExpression = function(expression) {
  this.request.FilterExpression = expression;

  return this;
};

Query.prototype.expressionAttributeValues = function(data) {
  this.request.ExpressionAttributeValues = data;

  return this;
};

Query.prototype.expressionAttributeNames = function(data) {
  this.request.ExpressionAttributeNames = data;

  return this;
};

Query.prototype.projectionExpression = function(data) {
  this.request.ProjectionExpression = data;

  return this;
};

Query.prototype.usingIndex = function (name) {
  this.request.IndexName = name;

  return this;
};

Query.prototype.consistentRead = function (read) {
  if(!_.isBoolean(read)) {
    read = true;
  }

  this.request.ConsistentRead = read;

  return this;
};

internals.addExpressionAttributes = function (request, condition) {
  var expressionAttributeNames  = _.merge({}, condition.attributeNames, request.ExpressionAttributeNames);
  var expressionAttributeValues = _.merge({}, condition.attributeValues, request.ExpressionAttributeValues);

  if (!_.isEmpty(expressionAttributeNames)) {
    request.ExpressionAttributeNames = expressionAttributeNames;
  }

  if (!_.isEmpty(expressionAttributeValues)) {
    request.ExpressionAttributeValues = expressionAttributeValues;
  }
};

Query.prototype.addKeyCondition = function (condition) {
  internals.addExpressionAttributes(this.request, condition);

  if (_.isString(this.request.KeyConditionExpression )) {
    this.request.KeyConditionExpression = this.request.KeyConditionExpression + ' AND (' + condition.statement + ')';
  } else {
    this.request.KeyConditionExpression = '(' + condition.statement + ')';
  }

  return this;
};

Query.prototype.addFilterCondition = function (condition) {
  internals.addExpressionAttributes(this.request, condition);

  if (_.isString(this.request.FilterExpression )) {
    this.request.FilterExpression = this.request.FilterExpression + ' AND (' + condition.statement + ')';
  } else {
    this.request.FilterExpression = '(' + condition.statement + ')';
  }

  return this;
};

Query.prototype.startKey = function (hashKey, rangeKey) {
  this.request.ExclusiveStartKey = this.serializer.buildKey(hashKey, rangeKey, this.table.schema);

  return this;
};

Query.prototype.attributes = function(attrs) {
  if(!_.isArray(attrs)) {
    attrs = [attrs];
  }

  var expressionAttributeNames = _.reduce(attrs, function (result, attr) {
    var path = '#' + attr;
    result[path] = attr;

    return result;
  }, {});

  this.request.ProjectionExpression = _.keys(expressionAttributeNames).join(',');
  this.request.ExpressionAttributeNames = _.merge({}, expressionAttributeNames, this.request.ExpressionAttributeNames);

  return this;
};

Query.prototype.ascending = function () {
  this.request.ScanIndexForward = true;

  return this;
};

Query.prototype.descending = function () {
  this.request.ScanIndexForward = false;

  return this;
};

Query.prototype.select = function (value) {
  this.request.Select = value;

  return this;
};

Query.prototype.returnConsumedCapacity = function (value) {
  if(_.isUndefined(value)) {
    value = 'TOTAL';
  }

  this.request.ReturnConsumedCapacity = value;

  return this;
};

Query.prototype.loadAll = function () {
  this.options.loadAll = true;

  return this;
};

Query.prototype.where = function (keyName) {
  return internals.keyCondition(keyName, this.table.schema, this);
};

Query.prototype.filter = function (keyName) {
  return internals.queryFilter(keyName, this.table.schema, this);
};

Query.prototype.exec = function(callback) {
  var self = this;

  this.addKeyCondition(this.buildKey());

  var runQuery = function (params, callback) {
    self.table.runQuery(params, callback);
  };

  return utils.paginatedRequest(self, runQuery, callback);
};

Query.prototype.buildKey = function () {
  var key = this.table.schema.hashKey;

  if(internals.isUsingGlobalIndex(this)) {
    key = this.table.schema.globalIndexes[this.request.IndexName].hashKey;
  }

  var existingValueKeys = _.keys(this.request.ExpressionAttributeValues);
  return expressions.buildFilterExpression(key, '=', existingValueKeys, this.hashKey);
};

internals.formatAttributeValue = function (val) {
  if(_.isDate(val)) {
    return val.toISOString();
  }

  return val;
};

Query.prototype.buildRequest = function () {
  return _.merge({}, this.request, {TableName: this.table.tableName()});
};
