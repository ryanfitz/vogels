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

Account.prototype.sayHello = function () {
  console.log('Hello my name is ' + this.get('name') + ' I\'m ' + this.get('age') + ' years old');
};

Account.findByAgeRange = function (low, high) {
  Account.scan()
  .where('age').gte(low)
  .where('age').lte(high)
  .loadAll()
  .exec(function (err, data) {
    data.Items.forEach(function (account) {
      account.sayHello();
    });
  });
};


