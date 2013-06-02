'use strict';

var vogels = require('../index'),
    AWS    = vogels.AWS;

AWS.config.loadFromPath(process.env.HOME + '/.ec2/credentials.json');

var Account = vogels.define('Account', function (schema) {
  schema.String('email', {hashKey: true});
  schema.String('name');
  schema.Number('age');
  schema.StringSet('nicknames');
});

Account.update({email : 'test5@example.com', age : {$add : 1}}, function (err, acc) {
  console.log('incremented age', acc.get('age'));
});

Account.update({email : 'test@example.com', nicknames : {$add : 'smalls'}}, function (err, acc) {
  console.log('added one nickname', acc.get('nicknames'));
});

Account.update({email : 'test@example.com', nicknames : {$add : ['bigs', 'big husk', 'the dude']}}, function (err, acc) {
  console.log('added three nicknames', acc.get('nicknames'));
});

Account.update({email : 'test@example.com', nicknames : {$del : 'the dude'}}, function (err, acc) {
  console.log('removed nickname', acc.get('nicknames'));
});
