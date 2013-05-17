'use strict';

var vogels = require('../index'),
    util  = require('util'),
    _     = require('lodash'),
    AWS    = vogels.AWS;

AWS.config.loadFromPath(process.env.HOME + '/.ec2/credentials.json');

var Account = vogels.define('Account', function (schema) {
  schema.String('name', {hashKey: true});
  schema.String('email', {rangeKey: true});
  schema.Date('created', {secondaryIndex: true});
  schema.Number('age');
});

var printResults = function (err, resp) {
  console.log('----------------------------------------------------------------------');
  if(err) {
    console.log('Error running query', err);
  } else {
    console.log('Found', resp.Count, 'items');
    console.log(util.inspect(_.pluck(resp.Items, 'attrs')));

    if(resp.ConsumedCapacity) {
      console.log('----------------------------------------------------------------------');
      console.log('Query consumed: ', resp.ConsumedCapacity);
    }
  }

  console.log('----------------------------------------------------------------------');
};

// Basic query against hash key
Account.query('Test').exec(printResults);

// Run query limiting returned items to 3
Account.query('Test').limit(3).exec(printResults);

// Query with rang key condition
Account.query('Test')
  .where('email').beginsWith('foo')
  .exec(printResults);

// Run query returning only email and created attributes
// also returns consumed capacity query took
Account.query('Test')
  .where('email').gte('a@example.com')
  .attributes(['email','created'])
  .returnConsumedCapacity()
  .exec(printResults);

// Run query against secondary index
Account.query('Test')
  .usingIndex('createdIndex')
  .where('created').lt(Date.now())
  .descending()
  .exec(printResults);
