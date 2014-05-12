'use strict';

var vogels = require('../index'),
    AWS    = vogels.AWS;

AWS.config.loadFromPath(process.env.HOME + '/.ec2/credentials.json');

var Account = vogels.define('Account', function (schema) {
  schema.String('email', {hashKey: true});
  schema.String('name');
  schema.Number('age');
  schema.Date('created', {default: Date.now});

  schema.tableName = function () {
    var d = new Date();
    return ['accounts', d.getFullYear(), d.getMonth() + 1].join('_');
  };
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

vogels.createTables(function (err) {
  if(err) {
    console.log('Failed to create tables', err);
  } else {
    console.log('tables created & active');
    Account.get('test@example.com', printAccountInfo);
    Account.get('foo@example.com', {ConsistentRead: true}, printAccountInfo);

    Account.create({email: 'test@example.com', name : 'test', age: 21}, printAccountInfo);
  }
});
