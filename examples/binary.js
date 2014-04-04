'use strict';

var vogels = require('../index'),
    fs     = require('fs'),
    AWS    = vogels.AWS;

AWS.config.loadFromPath(process.env.HOME + '/.ec2/credentials.json');

var File = vogels.define('File', function (schema) {
  schema.String('name', {hashKey: true});
  schema.Binary('data');

  schema.Date('created', {default: Date.now});
});

var printFileInfo = function (err, file) {
  if(err) {
    console.log('got error', err);
  } else if (file) {
    console.log('got file', file.get());
  } else {
    console.log('file not found');
  }
};

fs.readFile(__dirname + '/basic.js', function (err, data) {
  if (err)  {
    throw err;
  }

  File.create({name : 'basic.js', data: data}, printFileInfo);
});
