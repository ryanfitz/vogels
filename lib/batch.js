'use strict';

var _ = require('lodash'),
    async = require('async');

var internals = {};

internals.buildGetItemsRequest = function (tableName, keys, options) {
  options = options || {};

  var request = {};

  request[tableName] = {
    Keys : keys
  };

  if(options.ConsistentRead === true) {
    request[tableName].ConsistentRead = true;
  }

  if(options.AttributesToGet) {
    request[tableName].AttributesToGet = options.AttributesToGet;
  }

  return { RequestItems : request };
};

internals.serializeKeys = function (keys, table, serializer) {
  return keys.map(function (key) {
    return serializer.buildKey(key, null, table.schema);
  });
};

internals.mergeResponses = function (tableName, responses) {
  var base = {
    Responses : {},
    ConsumedCapacity : []
  };

  base.Responses[tableName] = [];

  return responses.reduce(function (memo, resp) {
    if(resp.Responses && resp.Responses[tableName]) {
      memo.Responses[tableName] = memo.Responses[tableName].concat(resp.Responses[tableName]);
    }

    return memo;
  }, base);
};

internals.paginatedRequest = function (request, table, options, callback) {
  var responses = [];

  var doFunc = function (callback) {

    table.runBatchGetItems(request, function (err, resp) {
      if(err && err.retryable) {
        return callback();
      } else if(err) {
        return callback(err);
      }

      request = null;

      // Detect if there were failed items that need to be retried.
      // resp.UnprocessedKeys is in this format:
      // { 'table_name': Keys: [{'keyName': {'S': 'key1'}}, {'keyName': {'S', 'key2'}}]}
      // http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html
      if (_.isObject(resp.UnprocessedKeys)) {
        if (resp.UnprocessedKeys[table.config.name] && _.isArray(resp.UnprocessedKeys[table.config.name].Keys)) {
          request = internals.buildGetItemsRequest(table.config.name, resp.UnprocessedKeys[table.config.name].Keys, options);
        }
      }

      responses.push(resp);

      return callback();
    });
  };

  var testFunc = function () {
    return request !== null;
  };

  var resulsFunc = function (err) {
    if(err) {
      return callback(err);
    }

    callback(null, internals.mergeResponses(table.config.name, responses));
  };

  async.doWhilst(doFunc, testFunc, resulsFunc);
};

internals.buckets = function (keys) {
  var buckets = [];

  while( keys.length ) {
    buckets.push( keys.splice(0, 100) );
  }

  return buckets;
};

internals.initialBatchGetItems = function (keys, table, serializer, options, callback) {
  var serializedKeys = internals.serializeKeys(keys, table, serializer);

  var request = internals.buildGetItemsRequest(table.config.name, serializedKeys, options);

  internals.paginatedRequest(request, table, options, function (err, data) {
    if(err) {
      return callback(err);
    }

    var dynamoItems = data.Responses[table.config.name];

    var items = _.map(dynamoItems, function(i) {
      return table.initItem(serializer.deserializeItem(table.schema, i));
    });

    return callback(null, items);
  });
};

internals.getItems = function (table, serializer) {

  return function (keys, options, callback) {

    if (typeof options === 'function' && !callback) {
      callback = options;
      options = {};
    }

    async.map(internals.buckets(_.clone(keys)), function (key, callback) {
      internals.initialBatchGetItems(key, table, serializer, options, callback);
    }, function (err, results) {
      if(err) {
        return callback(err);
      }

      return callback(null, _.flatten(results));
    });
  };

};

module.exports = function (table, serializer) {

  return {
    getItems : internals.getItems(table, serializer)
  };

};
