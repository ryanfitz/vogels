'use strict';

var Scan  = require('./scan'),
    async = require('async'),
    util  = require('util'),
    _     = require('lodash');

var internals = {};

var ParallelScan = module.exports = function (table, serializer, totalSegments) {
  Scan.call(this, table, serializer);

  this.totalSegments = totalSegments;
};

util.inherits(ParallelScan, Scan);

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

    return memo;
  }, result);

  if(merged.ConsumedCapacity.CapacityUnits === 0) {
    delete merged.ConsumedCapacity;
  }

  return merged;
};

ParallelScan.prototype.exec = function (callback) {
  var self = this;

  var scanFuncs = [];
  _.times(self.totalSegments, function(segment) {
    var scn = new Scan(self.table, self.serializer);
    scn.request = _.cloneDeep(self.request);

    scn = scn.segments(segment, self.totalSegments).loadAll();

    scanFuncs.push(scn.exec.bind(scn));
  });

  async.parallel(scanFuncs, function (err, responses) {
    if(err) {
      return callback(err);
    }

    return callback(null, internals.mergeResults(responses, self.table.config.name));
  });
};
