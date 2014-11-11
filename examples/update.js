'use strict';

var vogels = require('../index'),
    AWS    = vogels.AWS,
    Joi    = require('joi');

AWS.config.loadFromPath(process.env.HOME + '/.ec2/credentials.json');

var Account = vogels.define('example-update', {
  hashKey : 'email',
  timestamps : true,
  schema : {
    email : Joi.string().email(),
    name  : Joi.string(),
    age   : Joi.number(),
    nicknames : vogels.types.stringSet(),
    nested : Joi.object()
  }
});

vogels.createTables(function (err) {
  if(err) {
    console.log('Error creating tables', err);
    process.exit(1);
  }

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

  Account.update({email : 'test@example.com', nested : {roles : ['guest']}}, function (err, acc) {
    console.log('added nested data', acc.get('nested'));
  });

});
