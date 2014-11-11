'use strict';

var vogels = require('../index'),
    AWS    = vogels.AWS,
    Joi    = require('joi');

AWS.config.loadFromPath(process.env.HOME + '/.ec2/credentials.json');

var Account = vogels.define('example-hook', {
  hashKey : 'email',
  timestamps : true,
  schema : {
    email : Joi.string().email(),
    name  : Joi.string(),
    age   : Joi.number(),
  }
});

Account.before('create', function (data, next) {
  if(!data.name) {
    data.name = 'Foo Bar';
  }

  return next(null, data);
});

Account.before('update', function (data, next) {
  data.age = 45;
  return next(null, data);
});

Account.after('create', function (item) {
  console.log('Account created', item.get());
});

Account.after('update', function (item) {
  console.log('Account updated', item.get());
});

Account.after('destroy', function (item) {
  console.log('Account destroyed', item.get());
});

vogels.createTables(function (err) {
  if(err) {
    console.log('Error creating tables', err);
    process.exit(1);
  }

  Account.create({email: 'test11@example.com'}, function (err, acc) {
    acc.set({age: 25});

    acc.update(function () {
      acc.destroy({ReturnValues: 'ALL_OLD'});
    });

  });

});
