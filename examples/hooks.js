'use strict';

var vogels = require('../index'),
    AWS    = vogels.AWS;

AWS.config.loadFromPath(process.env.HOME + '/.ec2/vogels.json');

var Account = vogels.define('Account', function (schema) {
  schema.String('email', {hashKey: true});
  schema.String('name');
  schema.Number('age');
  schema.Date('created', {default: Date.now});
  schema.Date('updated');
});

Account.before('create', function (data, next) {
  if(!data.name) {
    data.name = 'Foo Bar';
  }

  return next(null, data);
});

Account.before('update', function (data, next) {

  setTimeout(function () {
    data.updated = Date.now();
    return next(null, data);
  }, 1000);

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

Account.create({email: 'test11@example.com'}, function (err, acc) {
  acc.set({age: 25});

  acc.update(function () {
    acc.destroy({ReturnValues: 'ALL_OLD'});
  });

});

