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
  }
  console.log('----------------------------------------------------------------------');
};

Account.query('Test').where('email').beginsWith('foo').exec(printResults);

Account.query('Test')
  .usingIndex('createdIndex')
  .where('created')
  .lt(Date.now())
  .descending()
  .exec(printResults);
