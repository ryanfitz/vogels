'use strict';

var _        = require('lodash'),
    Readable = require('stream').Readable,
    async    = require('async'),
    AWS      = require('aws-sdk');

var utils = module.exports;

utils.omitNulls = function (data) {
  return _.omit(data, function(value) {
    return _.isNull(value) ||
      _.isUndefined(value) ||
      (_.isArray(value) && _.isEmpty(value)) ||
      (_.isString(value) && _.isEmpty(value));
  });
};

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
  // if callback isn't passed switch to stream
  if(!callback) {
    return utils.streamRequest(self, runRequestFunc);
  }

  var lastEvaluatedKey = null;
  var responses = [];
  var retry = false;

  var doFunc = function (callback) {
    if(lastEvaluatedKey) {
      self.startKey(lastEvaluatedKey);
    }

    runRequestFunc(self.buildRequest(), function (err, resp) {
      if(err && err.retryable) {
        retry = true;
        return setImmediate(callback);
      } else if(err) {
        retry = false;
        return setImmediate(callback, err);
      }

      retry = false;
      lastEvaluatedKey = resp.LastEvaluatedKey;

      responses.push(resp);

      return setImmediate(callback);
    });
  };

  var testFunc = function () {
    return (self.options.loadAll && lastEvaluatedKey) || retry;
  };

  var resulsFunc = function (err) {
    if(err) {
      return callback(err);
    }

    return callback(null, utils.mergeResults(responses, self.table.tableName()));
  };

  async.doWhilst(doFunc, testFunc, resulsFunc);
};


utils.streamRequest = function (self, runRequestFunc) {
  var lastEvaluatedKey = null;
  var performRequest = true;

  var stream = new Readable({objectMode: true});

  var startRead = function () {
    if(!performRequest) {
      return;
    }

    if(lastEvaluatedKey) {
      self.startKey(lastEvaluatedKey);
    }

    runRequestFunc(self.buildRequest(), function (err, resp) {
      if(err && err.retryable) {
        return setTimeout(startRead, 1000);
      } else if(err) {
        return stream.emit('error', err);
      } else {
        lastEvaluatedKey = resp.LastEvaluatedKey;

        if(!self.options.loadAll || !lastEvaluatedKey) {
          performRequest = false;
        }

        stream.push(resp);

        if(!self.options.loadAll || !lastEvaluatedKey) {
          stream.push(null);
        }
      }

    });
  };

  stream._read = function () {
    startRead();
  };

  return stream;
};

utils.omitPrimaryKeys = function (schema, params) {
  return _.omit(params, schema.hashKey, schema.rangeKey);
};

utils.strToBin = function (value) {
  if (typeof(value) !== 'string') {
    var StrConversionError = 'Need to pass in string primitive to be converted to binary.';
    throw new Error(StrConversionError);
  }

  if (AWS.util.isBrowser()) {
    var len = value.length;
    var bin = new Uint8Array(new ArrayBuffer(len));
    for (var i = 0; i < len; i++) {
      bin[i] = value.charCodeAt(i);
    }
    return bin;
  } else {
    return AWS.util.Buffer(value);
  }
};
