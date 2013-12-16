'use strict';

var vogels = require('../index'),
    _     = require('lodash'),
    AWS    = vogels.AWS;

AWS.config.loadFromPath(process.env.HOME + '/.ec2/credentials.json');

// http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html

var GameScore = vogels.define('GameScore', function (schema) {
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

//GameScore.createTable(function (err, result) {
  //if(err) {
    //console.log('error creating table', err);
  //} else  {

    //console.log(err, result);

    //GameScore.describeTable(function (err, result) {
      //console.log('table info', result);
    //});
  //}
//});

var createGameScoreRecord = function (attrs) {
  GameScore.create(attrs, function (err) {
    if(err) {
      console.log('error creating game score record', err);
    }
  });
};

var data = [
  {userId: '101', gameTitle : 'Galaxy Invaders', topScore: 5842, wins: 10, losses: 5 , topScoreDateTime: new Date(2012, 1, 3, 8, 30)},
  {userId: '101', gameTitle : 'Meteor Blasters', topScore: 1000, wins: 12, losses: 3, topScoreDateTime: new Date(2013, 1, 3, 8, 30) },
  {userId: '101', gameTitle : 'Starship X', topScore: 24, wins: 4, losses: 9 },

  {userId: '102', gameTitle : 'Alien Adventure', topScore: 192, wins: 32, losses: 192 },
  {userId: '102', gameTitle : 'Galaxy Invaders', topScore: 0, wins: 0, losses: 5 },

  {userId: '103', gameTitle : 'Attack Ship', topScore: 3, wins: 1, losses: 8 },
  {userId: '103', gameTitle : 'Galaxy Invaders', topScore: 2317, wins: 40, losses: 3 },
  {userId: '103', gameTitle : 'Meteor Blasters', topScore: 723, wins: 22, losses: 12 },
  {userId: '103', gameTitle : 'Starship X', topScore: 42, wins: 4, losses: 19 },
];

_.each(data, createGameScoreRecord);

// Perform query against global secondary index
GameScore
  .query('Galaxy Invaders')
  .usingIndex('GameTitleIndex')
  .where('topScore').gt(0)
  .descending()
  .exec(function (err, data) {
    if(err){
      console.log(err);
    } else {
      console.log(_.map(data.Items, JSON.stringify));
    }
  });

