'use strict';

var _ = require('lodash'),
    async = require('async');

var internals = {};

internals.keyCondition = function (keyName, schema, scan) {

  var f = function (operator) {
    return function (value) {
      var cond = scan.buildAttributeValue(keyName, value, operator);
      return scan.addKeyCondition(cond);
    };
  };

  return {
    equals      : f('EQ'),
    eq          : f('NE'),
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
    this.request.ScanFilter = {};
  }

  this.request.ScanFilter = _.merge({}, this.request.ScanFilter, condition);

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
  this.request.Count = value;

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

internals.mergeResults = function (responses, tableName) {
  var result = {
    Items : [],
    ConsumedCapacity : {
      CapacityUnits : 0,
      TableName : tableName
    },
    Count : 0,
    ScannedCount : 0
  };

  var merged = _.reduce(responses, function (memo, resp) {
    memo.Count += resp.Count || 0;
    memo.ScannedCount += resp.ScannedCount || 0;

    if(resp.ConsumedCapacity) {
      memo.ConsumedCapacity.CapacityUnits += resp.ConsumedCapacity.CapacityUnits || 0;
    }

    if(resp.Items) {
      memo.Items = memo.Items.concat(resp.Items);
    }

    if(resp.LastEvaluatedKey) {
      memo.LastEvaluatedKey = resp.LastEvaluatedKey;
    }

    return memo;
  }, result);

  if(merged.ConsumedCapacity.CapacityUnits === 0) {
    delete merged.ConsumedCapacity;
  }

  return merged;
};

Scan.prototype.exec = function(callback) {
  var self = this;

  var lastEvaluatedKey = null;
  var responses = [];

  var doFunc = function (callback) {
    if(lastEvaluatedKey) {
      self.startKey(lastEvaluatedKey);
    }

    self.table.runScan(self.buildRequest(), function (err, resp) {
      if(err && err.retryable) {
        return callback();
      } else if(err) {
        return callback(err);
      }

      lastEvaluatedKey = resp.LastEvaluatedKey;
      responses.push(resp);

      return callback();
    });
  };

  var testFunc = function () {
    return self.options.loadAll && lastEvaluatedKey;
  };

  var resulsFunc = function (err) {
    if(err) {
      return callback(err);
    }

    callback(null, internals.mergeResults(responses, self.table.config.name));
  };

  async.doWhilst(doFunc, testFunc, resulsFunc);
};

Scan.prototype.loadAll = function () {
  this.options.loadAll = true;

  return this;
};

Scan.prototype.buildAttributeValue = function (key, value, operator) {
  var self = this;

  var result = {};

  if(_.isNull(value) || _.isUndefined(value)) {
    result[key] = {
      ComparisonOperator : operator
    };

    return result;
  }

  if(!_.isArray(value)) {
    value = [value];
  }

  var valueList = _.map(value, function (v) {
    var data = {};
    data[key] = v;

    var item = self.serializer.serializeItem(self.table.schema, data, {convertSets: true});

    return item[key];
  });

  result[key] = {
    AttributeValueList : valueList,
    ComparisonOperator : operator
  };

  return result;
};

Scan.prototype.buildRequest = function () {
  return _.merge({}, this.request, {TableName: this.table.config.name});
};
