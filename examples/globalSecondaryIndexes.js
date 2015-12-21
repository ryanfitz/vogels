'use strict';

var vogels = require('../index'),
    _      = require('lodash'),
    util   = require('util'),
    AWS    = vogels.AWS,
    Joi    = require('joi'),
    async  = require('async');

// AWS.config.loadFromPath(process.env.HOME + '/.ec2/credentials.json');

// http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html

AWS.config.update({region : 'us-east-1'});

var GameScore = vogels.define('example-global-index', {
  hashKey : 'userId',
  rangeKey : 'gameTitle',
  schema : {
    userId           : Joi.string(),
    gameTitle        : Joi.string(),
    topScore         : Joi.number(),
    topScoreDateTime : Joi.date(),
    wins             : Joi.number(),
    losses           : Joi.number()
  },
  indexes : [{
    hashKey : 'gameTitle',
    rangeKey : 'topScore',
    name : 'GameTitleIndex',
    type : 'global',
    projection: { NonKeyAttributes: [ 'wins' ], ProjectionType: 'INCLUDE' }
  },
  { hashKey : 'gameTitle', rangeKey : 'losses', name : 'GameLosersIndex', type : 'global'}
  ]
});

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

var loadSeedData = function (callback) {
  callback = callback || _.noop;

  async.each(data, function(attrs, callback) {
    GameScore.create(attrs,callback);
  }, callback);
};

async.series([
  async.apply(vogels.createTables.bind(vogels)),
  loadSeedData
], function (err) {
  if(err) {
    console.log('error', err);
    process.exit(1);
  }

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
      console.log('Found', data.Count, 'items');
      console.log(util.inspect(_.pluck(data.Items, 'attrs')));
    }
  });
});
