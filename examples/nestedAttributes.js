'use strict';

var vogels = require('../index'),
    util   = require('util'),
    _      = require('lodash'),
    async  = require('async'),
    Joi    = require('joi'),
    AWS    = vogels.AWS;

AWS.config.loadFromPath(process.env.HOME + '/.ec2/credentials.json');

var Movie = vogels.define('example-nested-attribute', {
  hashKey : 'title',
  timestamps : true,
  schema : {
    title       : Joi.string(),
    releaseYear : Joi.number(),
    tags        : vogels.types.stringSet(),
    director    : Joi.object().keys({
      firstName : Joi.string(),
      lastName  : Joi.string(),
      titles    : Joi.array()
    }),
    actors : Joi.array().includes(Joi.object().keys({
      firstName : Joi.string(),
      lastName  : Joi.string(),
      titles    : Joi.array()
    }))
  }
});

var printResults = function (err, data) {
  console.log('----------------------------------------------------------------------');
  if(err) {
    console.log('Error - ', err);
  } else {
    console.log('Movie - ', util.inspect(data.get(), {depth : null}));
  }
  console.log('----------------------------------------------------------------------');
};

var loadSeedData = function (callback) {
  callback = callback || _.noop;

  async.times(10, function(n, next) {
    var director = { firstName : 'Steven', lastName : 'Spielberg the ' + n, titles : ['Producer', 'Writer', 'Director']};
    var actors = [
      { firstName : 'Tom', lastName : 'Hanks', titles : ['Producer', 'Actor', 'Soundtrack']}
    ];

    var tags = ['tag ' + n];

    if(n %3 === 0) {
      actors.push({ firstName : 'Rex', lastName : 'Ryan', titles : ['Actor', 'Head Coach']});
      tags.push('Action');
    }

    if(n %5 === 0) {
      actors.push({ firstName : 'Tom', lastName : 'Coughlin', titles : ['Writer', 'Head Coach']});
      tags.push('Comedy');
    }

    Movie.create({title : 'Movie ' + n, releaseYear : 2001 + n, actors : actors, director : director, tags: tags}, next);
  }, callback);
};

var runExample = function () {

  Movie.create({
    title : 'Star Wars: Episode IV - A New Hope',
    releaseYear : 1977,
    director : {
      firstName : 'George', lastName : 'Lucas', titles : ['Director']
    },
    actors : [
      { firstName : 'Mark',     lastName : 'Hamill', titles : ['Actor']},
      { firstName : 'Harrison', lastName : 'Ford',   titles : ['Actor', 'Producer']},
      { firstName : 'Carrie',   lastName : 'Fisher', titles : ['Actress', 'Writer']},
    ],
    tags : ['Action', 'Adventure']
  }, printResults);

  var params = {};
  params.UpdateExpression = 'SET #year = #year + :inc, #dir.titles = list_append(#dir.titles, :title), #act[0].firstName = :firstName ADD tags :tag';
  params.ConditionExpression = '#year = :current';
  params.ExpressionAttributeNames = {
    '#year' : 'releaseYear',
    '#dir' : 'director',
    '#act' : 'actors'
  };
  params.ExpressionAttributeValues = {
    ':inc' : 1,
    ':current' : 2001,
    ':title' : ['The Man'],
    ':firstName' : 'Rob',
    ':tag' : vogels.Set(['Sports', 'Horror'], 'S')
  };

  Movie.update({title : 'Movie 0'}, params, printResults);
};

async.series([
  async.apply(vogels.createTables.bind(vogels)),
  loadSeedData
], function (err) {
  if(err) {
    console.log('error', err);
    process.exit(1);
  }

  runExample();
});
