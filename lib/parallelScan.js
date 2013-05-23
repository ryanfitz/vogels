'use strict';

var Scan     = require('./scan'),
    async    = require('async'),
    NodeUtil = require('util'),
    utils    = require('./utils'),
    _        = require('lodash');

var ParallelScan = module.exports = function (table, serializer, totalSegments) {
  Scan.call(this, table, serializer);

  this.totalSegments = totalSegments;
};

NodeUtil.inherits(ParallelScan, Scan);

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

    return callback(null, utils.mergeResults(responses, self.table.config.name));
  });
};
