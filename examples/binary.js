'use strict';

var vogels = require('../index'),
    fs     = require('fs'),
    AWS    = vogels.AWS,
    Joi    = require('joi');

AWS.config.loadFromPath(process.env.HOME + '/.ec2/credentials.json');

var BinModel = vogels.define('example-binary', {
  hashKey : 'name',
  timestamps : true,
  schema : {
    name : Joi.string(),
    data : Joi.binary()
  }
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

vogels.createTables(function (err) {
  if(err) {
    console.log('Error creating tables', err);
    process.exit(1);
  }

  fs.readFile(__dirname + '/basic.js', function (err, data) {
    if (err)  {
      throw err;
    }

    BinModel.create({name : 'basic.js', data: data}, printFileInfo);

  });
});
