'use strict';

var vogels = require('../index'),
    Joi    = require('joi'),
    AWS    = vogels.AWS;

AWS.config.loadFromPath(process.env.HOME + '/.ec2/credentials.json');

var Account = vogels.define('example-model-methods-Account', {
  hashKey : 'email',
  timestamps : true,
  schema : {
    email : Joi.string(),
    name : Joi.string(),
    age : Joi.number(),
  }
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


