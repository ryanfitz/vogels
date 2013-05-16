'use strict';

var vogels = require('../index'),
    AWS    = vogels.AWS;

AWS.config.loadFromPath(process.env.HOME + '/.ec2/credentials.json');

var Store = vogels.define('Dev_Store', function (schema) {
  schema.String('domain', {hashKey: true});
  schema.String('name');
  schema.Number('productCount');
});

Store.config({tableName: 'Dev_Stores'});

Store.get('katespade.com', function (err, str) {
  console.log('got', err, str.get());
});

Store.create({domain: 'foobars.com', name : 'Foo Bars', productCount: 15}, function (err, fooStore) {
  console.log('created', err, fooStore.get());

  fooStore.set({productCount: 22}).update(function (err) {
    console.log('updated', err, fooStore.get());

    fooStore.destroy(function () {
      console.log('destroyed store');
    });

  });
});

//Store.destroy('foobars.com', function (err) {
//console.log('destroyed',  err);
//});
