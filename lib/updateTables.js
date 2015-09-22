'use strict';

var async = require('async'),
    _     = require('lodash');

var internals = {};

internals.updateTable = function (model, options, callback) {
  options = options || {};

  var tableName = model.tableName();

  model.describeTable(function (err, data) {
    if(!_.isNull(data)) {
      var existingIndexes = _.pluck(data.Table.GlobalSecondaryIndexes, 'IndexName');
      var existingThroughput = data.Table.ProvisionedThroughput;
      console.log('updating table', tableName);
      return model.updateTable(options, existingIndexes, existingThroughput, function (error) {

        if(error) {
          console.error('failed to updated table ' + tableName, error);
          return callback(error);
        }

        console.log('waiting for table ' + tableName + ' to become ACTIVE');
        internals.waitTillActive(model, callback);
      });
    } else {
      console.log('table does not exist', tableName);
      return callback();
    }
  });
};

internals.waitTillActive = function (model, callback) {
  var status = 'PENDING';

  async.doWhilst(
    function (callback) {
      model.describeTable(function (err, data) {
        if(err) {
          return callback(err);
        }
        var indexStatuses = _.pluck(data.Table.GlobalSecondaryIndexes, 'IndexStatus');
        if (indexStatuses.every(function(element) { return element === 'ACTIVE'; })) {
          status = 'ACTIVE';
        }
        setTimeout(callback, 1000);
      });
    },
    function () { return status !== 'ACTIVE'; },
    function (err) {
      return callback(err);
    });
};

module.exports = function (models, config, callback) {
  async.eachSeries(_.keys(models), function (key, callback) {
    return internals.updateTable(models[key], config[key], callback);
  }, callback);
};
