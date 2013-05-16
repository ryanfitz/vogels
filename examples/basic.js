'use strict';

var vogels = require('../index'),
    AWS    = vogels.AWS;

AWS.config.loadFromPath(process.env.HOME + '/.ec2/credentials.json');

var Account = vogels.define('Account', function (schema) {
  schema.String('email', {hashKey: true});
  schema.String('name');
  schema.Number('age');
});

Account.get('test@example.com', function (err, acc) {
  if(err) {
    console.log('got error', err);
  } else if (acc) {
    console.log('got account', acc.get());
  } else {
    console.log('account not found');
  }
});

Account.create({email: 'test@example.com', name : 'Example Acc', age: 21}, function (err, acc) {
  console.log('account created', acc.get());

  acc.set({name: 'Example Account', age: 25}).update(function (err) {
    console.log('account updated', err, acc.get());
  });
});
