'use strict';

var vogels = require('../index'),
    _      = require('lodash'),
    util   = require('util'),
    AWS    = vogels.AWS,
    Joi    = require('joi');

AWS.config.loadFromPath(process.env.HOME + '/.ec2/credentials.json');

var Account = vogels.define('Foobar', {
  hashKey : 'email',
  schema : {
    email   : Joi.string(),
    name    : Joi.string(),
    age     : Joi.number(),
    scores  : vogels.types.numberSet(),
    created : Joi.date().default(Date.now),
    list    : Joi.array(),
    settings : {
      nickname    : Joi.string(),
      luckyNumber : Joi.number().min(1).default(7)
    }
  }
});

var printAccountInfo = function (err, acc) {
  if(err) {
    console.log('got error', err);
  } else if (acc) {
    console.log('got account', acc.get());
  } else {
    console.log('account not found');
  }
};

var printScanResults = function (err, data) {
  if(err) {
    console.log('got scan error', err);
  } else if (data.Items) {
    var items = _.map(data.Items, function (d) { return d.get(); });
    console.log('scan finished, got ', util.inspect(items, { showHidden: false, depth: null }));
  } else {
    console.log('scan returned empty result set');
  }
};

vogels.createTables(function (err) {
  if(err) {
    console.log('failed to create table', err);
  }

  // Simple get request
  Account.get('test11@example.com', printAccountInfo);
  Account.get('test@test.com', printAccountInfo);

  // Create an account
  var params = {
    email: 'test11@example.com', name : 'test 11', age: 21, scores : [22, 55, 44],
    list : ['a', 'b', 'c', 1, 2, 3],
    settings : {nickname : 'tester'}
  };

  Account.create(params, function (err, acc) {
    printAccountInfo(err, acc);

    acc.set({name: 'Test 11', age: 25}).update(function (err) {
      console.log('account updated', err, acc.get());
    });
  });

  Account.scan().exec(printScanResults);
});
