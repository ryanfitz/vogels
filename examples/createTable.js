'use strict';

var vogels = require('../index'),
    AWS    = vogels.AWS;

AWS.config.loadFromPath(process.env.HOME + '/.ec2/credentials.json');

vogels.define('Account', function (schema) {
  schema.String('name', {hashKey: true});
  schema.String('email', {rangeKey: true});
  schema.Number('age', {secondaryIndex: true});
  schema.Date('created', {default: Date.now});
});

vogels.define('GameScore', function (schema) {
  schema.String('userId', {hashKey: true});
  schema.String('gameTitle', {rangeKey: true});
  schema.Number('topScore');
  schema.Date('topScoreDateTime');
  schema.Number('wins');
  schema.Number('losses');

  schema.globalIndex('GameTitleIndex', {
    hashKey: 'gameTitle',
    rangeKey: 'topScore',
    Projection: { NonKeyAttributes: [ 'wins' ], ProjectionType: 'INCLUDE' } //optional, defaults to ALL
  });
});

//Account.createTable(function (err, result) {
  //if(err) {
    //console.log('error create table', err);
  //} else  {

    //console.log(err, result);

    //Account.describeTable(function (err, result) {
      //console.log('table info', result);
    //});
  //}
//});

vogels.createTables({
  'Account'   : {readCapacity: 1, writeCapacity: 1},
  'GameScore' : {readCapacity: 1, writeCapacity: 1}
}, function (err) {
  if(err) {
    console.log('Error creating tables', err);
  } else {
    console.log('table are now created and active');
  }
});
