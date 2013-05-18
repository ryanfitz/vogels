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
  schema.NumberSet('scores');
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

// Basic scan against table
Account.scan().exec(printResults);

// Run scan limiting returned items to 4
Account.scan().limit(4).exec(printResults);

// Scan with key condition
Account.scan()
  .where('email').beginsWith('test5')
  .exec(printResults);

// Run scan returning only email and created attributes
// also returns consumed capacity the scan took
Account.scan()
  .where('email').gte('f@example.com')
  .attributes(['email','created'])
  .returnConsumedCapacity()
  .exec(printResults);

Account.scan()
  .where('scores').contains(2)
  .exec(printResults);
