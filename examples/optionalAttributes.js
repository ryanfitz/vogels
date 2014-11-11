'use strict';

var vogels = require('../index'),
    AWS    = vogels.AWS,
    Joi    = require('joi');

AWS.config.loadFromPath(process.env.HOME + '/.ec2/credentials.json');

var Person = vogels.define('example-optional-attribute', {
  hashKey : 'id',
  schema : {
    id : vogels.types.uuid(),
    name : Joi.string().allow(null)
  }
});

var printInfo = function (err, person) {
  if(err) {
    console.log('got error', err);
  } else if (person) {
    console.log('got person', person.get());
  } else {
    console.log('person not found');
  }
};

vogels.createTables( function (err) {
  if(err) {
    console.log('Failed to create table', err);
    process.exit(1);
  }

  Person.create({name : 'Nick'}, printInfo);
  Person.create({name : null}, printInfo);

  var p = new Person({name : null});
  p.save(printInfo);
});
