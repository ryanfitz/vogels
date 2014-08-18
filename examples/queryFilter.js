'use strict';

var vogels = require('../index'),
    util  = require('util'),
    _     = require('lodash'),
    AWS    = vogels.AWS;

AWS.config.loadFromPath(process.env.HOME + '/.ec2/credentials.json');

var Account = vogels.define('ExampleAccount', function (schema) {
  schema.String('name', {hashKey: true});
  schema.String('email', {rangeKey: true});
  schema.Date('created', {secondaryIndex: true});
  schema.Number('age');
  schema.StringSet('roles');
});

var printResults = function (msg) {
  return function (err, resp) {

    console.log('----------------------------------------------------------------------');
    if(err) {
      console.log('Error running query', err);
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

var loadSeedData = function () {
  _.times(30, function(n) {
    var roles = n %3 === 0 ? ['admin', 'editor'] : ['user'];
    Account.create({email: 'test' + n + '@example.com', name : 'Test ' + n %3, age: n, roles : roles}, _.noop);
  });
};

var runFilterQueries = function () {

  // Basic equals filter
  Account.query('Test 1').filter('age').equals(4).exec(printResults('Equals Filter'));


  // between filter
  Account.query('Test 1').filter('age').between([5, 10]).exec(printResults('Between Filter'));

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

vogels.createTables(function (err) {
  if(err) {
    console.log('Error creating tables', err);
    process.exit();
  } else {
    console.log('table are now created and active');
    loadSeedData();
    runFilterQueries();
  }
});
