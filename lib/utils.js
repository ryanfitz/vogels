'use strict';

var _     = require('lodash'),
    async = require('async');

var utils = module.exports;

utils.mergeResults = function (responses, tableName) {
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

  if(merged.ScannedCount === 0) {
    delete merged.ScannedCount;
  }

  return merged;
};

utils.paginatedRequest = function (self, runRequestFunc, callback) {
  var lastEvaluatedKey = null;
  var responses = [];

  var doFunc = function (callback) {
    if(lastEvaluatedKey) {
      self.startKey(lastEvaluatedKey);
    }

    runRequestFunc(self.buildRequest(), function (err, resp) {
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

    callback(null, utils.mergeResults(responses, self.table.config.name));
  };

  async.doWhilst(doFunc, testFunc, resulsFunc);
};
