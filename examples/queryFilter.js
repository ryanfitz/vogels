'use strict';

var vogels = require('../index'),
    util   = require('util'),
    _      = require('lodash'),
    Joi    = require('joi'),
    async  = require('async'),
    AWS    = vogels.AWS;

AWS.config.loadFromPath(process.env.HOME + '/.ec2/credentials.json');

var Account = vogels.define('example-query-filter', {
  hashKey : 'name',
  rangeKey : 'email',
  timestamps : true,
  schema : {
    name  : Joi.string(),
    email : Joi.string().email(),
    age   : Joi.number(),
    roles : vogels.types.stringSet(),
  },

  indexes : [
    {hashKey : 'name', rangeKey : 'createdAt', type : 'local', name : 'CreatedAtIndex'}
  ]
});

var printResults = function (msg) {
  return function (err, resp) {

    console.log('----------------------------------------------------------------------');
    if(err) {
      console.log(msg + ' - Error running query', err);
    } else {
      console.log(msg + ' - Found', resp.Count, 'items');
      console.log(util.inspect(_.pluck(resp.Items, 'attrs')));

      if(resp.ConsumedCapacity) {
        console.log('----------------------------------------------------------------------');
        console.log('Query consumed: ', resp.ConsumedCapacity);
      }
    }

    console.log('----------------------------------------------------------------------');
  };
};

var loadSeedData = function (callback) {
  callback = callback || _.noop;

  async.times(30, function(n, next) {
    var roles = n %3 === 0 ? ['admin', 'editor'] : ['user'];
    Account.create({email: 'test' + n + '@example.com', name : 'Test ' + n %3, age: n, roles : roles}, next);
  }, callback);
};

var runFilterQueries = function () {

  // Basic equals filter
  Account.query('Test 1').filter('age').equals(4).exec(printResults('Equals Filter'));


  // between filter
  Account.query('Test 1').filter('age').between(5, 10).exec(printResults('Between Filter'));

  // IN filter
  Account.query('Test 1').filter('age').in([5, 10]).exec(printResults('IN Filter'));

  // exists filters
  Account.query('Test 1').filter('age').exists().exec(printResults('Exists Filter'));
  Account.query('Test 1').filter('age').exists(false).exec(printResults('NOT Exists Filter'));

  // contains filter
  Account.query('Test 0').filter('roles').contains('admin').exec(printResults('contains admin Filter'));

  // not contains filter
  Account.query('Test 1').filter('roles').notContains('admin').exec(printResults('NOT contains admin Filter'));
};

async.series([
  async.apply(vogels.createTables.bind(vogels)),
  loadSeedData
], function (err) {
  if(err) {
    console.log('error', err);
    process.exit(1);
  }

  runFilterQueries();
});
