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

// Simple get request
Account.get('test@example.com', printAccountInfo);

// Consistent Read get request
Account.get('foo@example.com', {ConsistentRead: true}, printAccountInfo);

// Create an account
Account.create({email: 'test11@example.com', name : 'test 11', age: 21}, function (err, acc) {
  console.log('account created', acc.get());

  acc.set({name: 'Test 11', age: 25}).update(function (err) {
    console.log('account updated', err, acc.get());
  });
});
