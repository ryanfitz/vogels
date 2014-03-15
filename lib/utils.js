'use strict';

var _        = require('lodash'),
    Readable = require('readable-stream'),
    async    = require('async');

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
    if(!resp) {
      return memo;
    }

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
  var streamMode = false;
  var responses = [];

  var stream = new Readable({objectMode: true});
  var performRequest = true;

  stream._read = function() {
    performRequest = true;
  };

  // if callback isn't passed in switch over to stream mode
  // and emit error event on the stream
  //
  if(!callback) {
    streamMode = true;
    callback = function (err) {
      if(err) {
        stream.emit('error', err);
      }
    };
  }

  var doFunc = function (callback) {
    if(lastEvaluatedKey) {
      self.startKey(lastEvaluatedKey);
    }

    if(!performRequest) {
      return setImmediate(callback);
    }

    runRequestFunc(self.buildRequest(), function (err, resp) {
      if(err && err.retryable) {
        return setImmediate(callback);
      } else if(err) {
        return setImmediate(callback, err);
      }

      lastEvaluatedKey = resp.LastEvaluatedKey;

      if(streamMode && !stream.push(resp)) {
        performRequest = false;
      } else if (!streamMode) {
        responses.push(resp);
      }

      return setImmediate(callback);
    });
  };

  var testFunc = function () {
    return self.options.loadAll && lastEvaluatedKey;
  };

  var resulsFunc = function (err) {
    if(err) {
      return callback(err);
    }

    stream.push(null);
    callback(null, utils.mergeResults(responses, self.table.config.name));
  };

  async.doWhilst(doFunc, testFunc, resulsFunc);

  return stream;
};
