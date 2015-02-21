'use strict';

var _     = require('lodash'),
    utils = require('./utils');

var internals = {};

internals.keyCondition = function (keyName, schema, query) {

  var f = function (operator) {
    return function (/*values*/) {
      var copy = [].slice.call(arguments);
      var args = [keyName, operator].concat(copy);
      var cond = query.buildAttributeValue.apply(query, args);
      return query.addKeyCondition(cond);
    };
  };

  return {
    equals     : f('EQ'),
    lte        : f('LE'),
    lt         : f('LT'),
    gte        : f('GE'),
    gt         : f('GT'),
    beginsWith : f('BEGINS_WITH'),
    between    : f('BETWEEN')
  };
};

internals.queryFilter = function (keyName, schema, query) {

  var f = function (operator) {
    return function (value) {
      if(operator === 'NOT_NULL') {
        if(_.isBoolean(value) && value === false) {
          operator = 'NULL';
        }

        var nullCond = query.table.docClient.Condition(keyName, operator);
        return query.addFilterCondition(nullCond);
      } else {
        var copy = [].slice.call(arguments);
        var args = [keyName, operator].concat(copy);
        var cond = query.buildAttributeValue.apply(query, args);
        return query.addFilterCondition(cond);
      }
    };
  };

  return {
    equals     : f('EQ'),
    notEquals  : f('NE'),
    lte        : f('LE'),
    lt         : f('LT'),
    gte        : f('GE'),
    gt         : f('GT'),
    exists     : f('NOT_NULL'),
    contains   : f('CONTAINS'),
    notContains: f('NOT_CONTAINS'),
    beginsWith : f('BEGINS_WITH'),
    'in'       : f('IN'),
    between    : f('BETWEEN')
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

Query.prototype.addKeyCondition = function (condition) {
  if(!this.request.KeyConditions) {
    this.request.KeyConditions = [];
  }

  this.request.KeyConditions.push(condition);

  return this;
};

Query.prototype.addFilterCondition = function (condition) {
  if(!this.request.QueryFilter) {
    this.request.QueryFilter = [];
  }

  this.request.QueryFilter.push(condition);

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

  this.request.AttributesToGet = attrs;

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

  return this.buildAttributeValue(key, 'EQ', this.hashKey);
};

internals.formatAttributeValue = function (val) {
  if(_.isDate(val)) {
    return val.toISOString();
  }

  return val;
};

Query.prototype.buildAttributeValue = function (key, operator, val1, val2) {
  var self = this;

  var v1 = internals.formatAttributeValue(val1);
  var v2 = internals.formatAttributeValue(val2);
  return self.table.docClient.Condition(key, operator, v1, v2);
};

Query.prototype.buildRequest = function () {
  this.addKeyCondition(this.buildKey());

  return _.merge({}, this.request, {TableName: this.table.tableName()});
};
