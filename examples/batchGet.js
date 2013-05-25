'use strict';

var vogels = require('../index'),
    AWS    = vogels.AWS;

AWS.config.loadFromPath(process.env.HOME + '/.ec2/credentials.json');

var Account = vogels.define('Account', function (schema) {
  schema.String('email', {hashKey: true});
  schema.String('name');
  schema.Number('age');
  schema.Date('created', {default: Date.now});
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

// Get two accounts at once
Account.batchGetItems(['test5@example.com', 'test4@example.com'], function (err, accounts) {
  accounts.forEach(function (acc) {
    printAccountInfo(null, acc);
  });
});

// Same as above but a strongly consistent read is used
Account.batchGetItems(['test5@example.com', 'test4@example.com'], {ConsistentRead: true}, function (err, accounts) {
  accounts.forEach(function (acc) {
    printAccountInfo(null, acc);
  });
});

// Get two accounts, but only fetching the age attribute
Account.batchGetItems(['test5@example.com', 'test4@example.com'], {AttributesToGet : ['age']}, function (err, accounts) {
  accounts.forEach(function (acc) {
    printAccountInfo(null, acc);
  });
});
