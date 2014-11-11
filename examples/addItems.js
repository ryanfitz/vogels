'use strict';

var vogels = require('../index'),
    AWS    = vogels.AWS,
    Joi    = require('joi'),
    async  = require('async');

AWS.config.loadFromPath(process.env.HOME + '/.ec2/credentials.json');

var Account = vogels.define('example-Account', {
  hashKey : 'AccountId',
  timestamps : true,
  schema : {
    AccountId : vogels.types.uuid(),
    name : Joi.string(),
    email : Joi.string().email(),
    age : Joi.number(),
  }
});

vogels.createTables({
  'example-Account'  : {readCapacity: 1, writeCapacity: 10},
}, function (err) {
  if(err) {
    console.log('Error creating tables', err);
    process.exit(1);
  }

  async.times(25, function(n, next) {
    Account.create({name : 'Account ' + n, email : 'account' +n + '@gmail.com', age : n}, next);
  });
});
