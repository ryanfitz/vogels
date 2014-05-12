'use strict';

var vogels = require('../index'),
    AWS    = vogels.AWS;

AWS.config.loadFromPath(process.env.HOME + '/.ec2/credentials.json');

var Person = vogels.define('Person', function (schema) {
  schema.UUID('id', {hashKey: true});
  schema.String('name').allow(null);
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
    return console.log('Failed to create table', err);
  }

  Person.create({name : 'Nick'}, printInfo);
  Person.create({name : null}, printInfo);

  var p = new Person({name : null});
  p.save(printInfo);
});
