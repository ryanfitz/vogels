'use strict';

var async = require('async'),
    _     = require('lodash');

var internals = {};

internals.createTable = function (model, options, callback) {
  options = options || {};

  var tableName = model.tableName();

  model.describeTable(function (err, data) {
    if(_.isNull(data)) {
      console.log('creating table', tableName);
      return model.createTable(options, function () {
        console.log('waiting for table ' + tableName + ' to become ACTIVE');
        internals.waitTillActive(model, callback);
      });
    } else {
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

      status = data.Table.TableStatus;

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
    return internals.createTable(models[key], config[key], callback);
  }, callback);
};
