'use strict';

var vogels = require('../index'),
    AWS    = vogels.AWS;

AWS.config.loadFromPath(process.env.HOME + '/.ec2/credentials.json');

var Account = vogels.define('Account', function (schema) {
  schema.String('name', {hashKey: true});
  schema.String('email', {rangeKey: true});
  schema.Number('age', {secondaryIndex: true});
  schema.Date('created', {default: Date.now});
});

for (var i = 0; i < 50 ; i++) {
  Account.create({name : 'Account ' + i, email : 'account' +i + '@gmail.com', age : i});
}
