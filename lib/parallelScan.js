'use strict';

var Scan     = require('./scan'),
async    = require('async'),
NodeUtil = require('util'),
utils    = require('./utils'),
Readable = require('readable-stream'),
_        = require('lodash');

var ParallelScan = module.exports = function (table, serializer, totalSegments) {
  Scan.call(this, table, serializer);

  this.totalSegments = totalSegments;
};

NodeUtil.inherits(ParallelScan, Scan);

ParallelScan.prototype.exec = function (callback) {
  var self = this;

  var streamMode = false;
  var combinedStream = new Readable({objectMode: true});

  combinedStream._read = function () {
  };

  if(!callback) {
    streamMode = true;
    callback = function (err) {
      if(err) {
        combinedStream.emit('error', err);
      }
    };
  }

  var scanFuncs = [];
  _.times(self.totalSegments, function(segment) {
    var scn = new Scan(self.table, self.serializer);
    scn.request = _.cloneDeep(self.request);

    scn = scn.segments(segment, self.totalSegments).loadAll();

    var scanFunc = function (callback) {
      if(streamMode) {
        var stream = scn.exec();

        stream.on('readable', function () {
          var data = stream.read();
          if(data) {
            combinedStream.push(data);
          }
        });

        stream.on('end', callback);

      } else {
        return scn.exec(callback);
      }
    };

    scanFuncs.push(scanFunc);
  });

  async.parallel(scanFuncs, function (err, responses) {
    if(err) {
      return callback(err);
    }

    combinedStream.push(null);
    return callback(null, utils.mergeResults(responses, self.table.tableName()));
  });

  return combinedStream;
};
