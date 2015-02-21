'use strict';

var _     = require('lodash'),
    utils = require('./utils');

var internals = {};

internals.keyCondition = function (keyName, schema, scan) {

  var f = function (operator) {
    return function (/*values*/) {
      var copy = [].slice.call(arguments);
      var args = [keyName, operator].concat(copy);
      var cond = scan.buildAttributeValue.apply(scan, args);
      return scan.addKeyCondition(cond);
    };
  };

  return {
    equals      : f('EQ'),
    eq          : f('EQ'),
    ne          : f('NE'),
    lte         : f('LE'),
    lt          : f('LT'),
    gte         : f('GE'),
    gt          : f('GT'),
    null        : f('NULL'),
    notNull     : f('NOT_NULL'),
    contains    : f('CONTAINS'),
    notContains : f('NOT_CONTAINS'),
    in          : f('IN'),
    beginsWith  : f('BEGINS_WITH'),
    between     : f('BETWEEN')
  };
};

var Scan = module.exports = function (table, serializer) {
  this.table = table;
  this.serializer = serializer;
  this.options = {loadAll: false};

  this.request = {};
};

Scan.prototype.limit = function(num) {
  if(num <= 0 ) {
    throw new Error('Limit must be greater than 0');
  }

  this.request.Limit = num;

  return this;
};

Scan.prototype.addKeyCondition = function (condition) {
  if(!this.request.ScanFilter) {
    this.request.ScanFilter = [];
  }

  this.request.ScanFilter.push(condition) ;

  return this;
};

Scan.prototype.startKey = function (hashKey, rangeKey) {
  this.request.ExclusiveStartKey = this.serializer.buildKey(hashKey, rangeKey, this.table.schema);

  return this;
};

Scan.prototype.attributes = function(attrs) {
  if(!_.isArray(attrs)) {
    attrs = [attrs];
  }

  this.request.AttributesToGet = attrs;

  return this;
};

Scan.prototype.select = function (value) {
  this.request.Select = value;

  return this;
};

Scan.prototype.returnConsumedCapacity = function (value) {
  if(_.isUndefined(value)) {
    value = 'TOTAL';
  }

  this.request.ReturnConsumedCapacity = value;

  return this;
};

Scan.prototype.segments = function (segment, totalSegments) {
  this.request.Segment = segment;
  this.request.TotalSegments = totalSegments;

  return this;
};


Scan.prototype.where = function (keyName) {
  return internals.keyCondition(keyName, this.table.schema, this);
};


Scan.prototype.filterExpression = function(expression) {
  this.request.FilterExpression = expression;

  return this;
};

Scan.prototype.expressionAttributeValues = function(data) {
  this.request.ExpressionAttributeValues = data;

  return this;
};

Scan.prototype.expressionAttributeNames = function(data) {
  this.request.ExpressionAttributeNames = data;

  return this;
};

Scan.prototype.projectionExpression = function(data) {
  this.request.ProjectionExpression = data;

  return this;
};

Scan.prototype.exec = function(callback) {
  var self = this;

  var runScan = function (params, callback) {
    self.table.runScan(params, callback);
  };

  return utils.paginatedRequest(self, runScan, callback);
};

Scan.prototype.loadAll = function () {
  this.options.loadAll = true;

  return this;
};

internals.formatAttributeValue = function (val) {
  if(_.isDate(val)) {
    return val.toISOString();
  }

  return val;
};

Scan.prototype.buildAttributeValue = function (key, operator, val1, val2) {
  var self = this;

  var v1 = internals.formatAttributeValue(val1);
  var v2 = internals.formatAttributeValue(val2);
  return self.table.docClient.Condition(key, operator, v1, v2);
};


Scan.prototype.buildRequest = function () {
  return _.merge({}, this.request, {TableName: this.table.tableName()});
};
