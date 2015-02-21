'use strict';

var vogels = require('../index'),
    AWS    = vogels.AWS,
    Joi    = require('joi'),
    async  = require('async'),
    util   = require('util'),
    _      = require('lodash');

AWS.config.loadFromPath(process.env.HOME + '/.ec2/credentials.json');

var DynamicModel = vogels.define('example-dynamic-key', {
  hashKey    : 'id',
  timestamps : true,
  schema : Joi.object().keys({
    id : Joi.string()
  }).unknown()
});

var printResults = function (err, resp) {
  console.log('----------------------------------------------------------------------');
  if(err) {
    console.log('Error running scan', err);
  } else {
    console.log('Found', resp.Count, 'items');
    console.log(util.inspect(_.pluck(resp.Items, 'attrs')));

    if(resp.ConsumedCapacity) {
      console.log('----------------------------------------------------------------------');
      console.log('Scan consumed: ', resp.ConsumedCapacity);
    }
  }

  console.log('----------------------------------------------------------------------');
};

vogels.createTables({
  'example-Account'  : {readCapacity: 1, writeCapacity: 10},
}, function (err) {
  if(err) {
    console.log('Error creating tables', err);
    process.exit(1);
  }

  async.times(25, function(n, next) {
    var data = {id : 'Model ' + n};

    if(n % 3 === 0) {
      data.name = 'Dynamic Model the 3rd';
      data.age = 33;
    }

    if(n % 5 === 0) {
      data.email = 'model_' + n + '@test.com';
      data.settings = { nickname : 'Model the 5th' };
    }

    DynamicModel.create(data, next);
  }, function () {

    DynamicModel.scan().loadAll().exec(printResults);
  });
});
