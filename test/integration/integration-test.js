'use strict';

var vogels = require('../../index'),
    chai   = require('chai'),
    expect = chai.expect,
    async  = require('async'),
    _      = require('lodash'),
    helper = require('../test-helper'),
    uuid   = require('node-uuid'),
    Joi    = require('joi');

chai.should();

var User, Tweet, Movie, DynamicKeyModel; // models
var internals = {};

internals.userId = function (n) {
  return 'userid-' + n;
};

internals.loadSeedData = function (callback) {
  callback = callback || _.noop;

  async.parallel([
    function (callback) {
    async.times(15, function(n, next) {
      var roles = ['user'];
      if(n % 3 === 0) {
        roles = ['admin', 'editor'];
      } else if (n %5 === 0) {
        roles = ['qa', 'dev'];
      }

      User.create({id : internals.userId(n), email: 'test' + n + '@example.com', name : 'Test ' + n %3, age: n +10, roles : roles}, next);
    }, callback);
  },
  function (callback) {
    async.times(15 * 5, function(n, next) {
      var userId = internals.userId( n %5);
      var p = {UserId : userId, content: 'I love tweeting, in fact Ive tweeted ' + n + ' times', num : n};
      if(n %3 === 0 ) {
        p.tag = '#test';
      }

      return Tweet.create(p, next);
    }, callback);
  },
  function (callback) {
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
  },
  ], callback);
};

describe('Vogels Integration Tests', function() {
  this.timeout(0);

  before(function (done) {
    vogels.dynamoDriver(helper.realDynamoDB());

    User = vogels.define('vogels-int-test-user', {
      hashKey : 'id',
      schema : {
        id            : Joi.string().required().default(uuid.v4),
        email         : Joi.string().required(),
        name          : Joi.string().allow(''),
        age           : Joi.number().min(10),
        roles         : vogels.types.stringSet().default(['user']),
        acceptedTerms : Joi.boolean().default(false),
        things        : Joi.array(),
        settings : {
          nickname : Joi.string(),
          notify : Joi.boolean().default(true),
          version : Joi.number()
        }

      }
    });

    Tweet = vogels.define('vogels-int-test-tweet', {
      hashKey  : 'UserId',
      rangeKey : 'TweetID',
      schema : {
        UserId            : Joi.string(),
        TweetID           : vogels.types.uuid(),
        content           : Joi.string(),
        num               : Joi.number(),
        tag               : Joi.string(),
        PublishedDateTime : Joi.date().default(Date.now)
      },
      indexes : [
        { hashKey : 'UserId', rangeKey : 'PublishedDateTime', type : 'local', name : 'PublishedDateTimeIndex'}
      ]
    });

    Movie = vogels.define('vogels-int-test-movie', {
      hashKey : 'title',
      timestamps : true,
      schema : {
        title       : Joi.string(),
        description : Joi.string(),
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

    DynamicKeyModel = vogels.define('vogels-int-test-dyn-key', {
      hashKey  : 'id',
      schema : Joi.object().keys({
        id : Joi.string()
      }).unknown()
    });

    async.series([
      async.apply(vogels.createTables.bind(vogels)),
      function (callback) {
        var items = [{fiz : 3, buz : 5, fizbuz: 35}];
        User.create({id : '123456789', email : 'some@user.com', age: 30, settings : {nickname : 'thedude'}, things : items}, callback);
      },
      function (callback) {
        User.create({id : '9999', email : '9999@test.com', age: 99, name: 'Nancy Nine'}, callback);
      },
      internals.loadSeedData
    ], done);
  });

  describe('#create', function () {
    it('should create item with hash key', function(done) {
      User.create({
        email : 'foo@bar.com',
        age : 18,
        roles : ['user', 'admin'],
        acceptedTerms : true,
        settings : {
          nickname : 'fooos',
          version : 2
        }
      }, function (err, acc) {
        expect(err).to.not.exist;
        expect(acc).to.exist;
        expect(acc.get()).to.have.keys(['id', 'email', 'age', 'roles', 'acceptedTerms', 'settings']);
        return done();
      });
    });

    it('should create item with empty string', function(done) {
      User.create({
        email : 'foo2@bar.com',
        name : '',
        age : 22,
        roles : ['user'],
        settings : {
          nickname : 'foo2',
          version : 2
        }
      }, function (err, acc) {
        expect(err).to.not.exist;
        expect(acc).to.exist;
        expect(acc.get()).to.have.keys(['id', 'email', 'age', 'roles', 'acceptedTerms', 'settings']);
        return done();
      });
    });

    it('should return condition exception when using ConditionExpression', function(done) {
      var item = { email : 'test123@test.com', age : 33, roles : ['user'] };

      User.create(item, function (err, acc) {
        expect(err).to.not.exist;
        expect(acc).to.exist;
        expect(acc.get('email')).to.eql('test123@test.com');

        var params = {};
        params.ConditionExpression = '#i <> :x';
        params.ExpressionAttributeNames = {'#i' : 'id'};
        params.ExpressionAttributeValues = {':x' : acc.get('id')};

        var item2 = _.merge(item, {id : acc.get('id')});
        User.create(item2, params, function (error, acc) {
          expect(error).to.exist;
          expect(error.code).to.eql('ConditionalCheckFailedException');
          expect(acc).to.not.exist;

          return done();
        });
      });
    });

    it('should return condition exception when using expected shorthand', function(done) {
      var item = { email : 'test444@test.com', age : 33, roles : ['user'] };

      User.create(item, function (err, acc) {
        expect(err).to.not.exist;
        expect(acc).to.exist;
        expect(acc.get('email')).to.eql('test444@test.com');

        var opts = {expected : {email : 'foo@bar.com'}};

        var item2 = _.merge(item, {id : acc.get('id')});
        User.create(item2, opts, function (error, acc) {
          expect(error).to.exist;
          expect(error.code).to.eql('ConditionalCheckFailedException');
          expect(acc).to.not.exist;

          return done();
        });
      });
    });

    it('should return condition exception when using overwrite shorthand', function(done) {
      var item = { email : 'testOverwrite@test.com', age : 20};

      User.create(item, function (err, acc) {
        expect(err).to.not.exist;
        expect(acc).to.exist;

        var item2 = _.merge(item, {id : acc.get('id')});
        User.create(item2, {overwrite : false}, function (error, acc) {
          expect(error).to.exist;
          expect(error.code).to.eql('ConditionalCheckFailedException');
          expect(acc).to.not.exist;

          return done();
        });
      });
    });

    it('should create item with dynamic keys', function(done) {
      DynamicKeyModel.create({
        id : 'rand-1',
        name : 'Foo Bar',
        children : ['sam', 'steve', 'sarah', 'sally'],
        settings : { nickname : 'Tester', info : { color : 'green', age : 19 } }
      }, function (err, acc) {
        expect(err).to.not.exist;
        expect(acc).to.exist;
        expect(acc.get()).to.have.keys(['id', 'name', 'children', 'settings']);
        return done();
      });
    });

    it('should create multiple items at once', function (done) {
      var item = { email : 'testMulti1@test.com', age : 10};
      var item2 = { email : 'testMulti2@test.com', age : 20};
      var item3 = { email : 'testMulti3@test.com', age : 30};

      User.create([item, item2, item3], function (err, accounts) {
        expect(err).to.not.exist;
        expect(accounts).to.exist;
        expect(accounts).to.have.length(3);

        return done();
      });
    });
  });

  describe('#get', function () {
    it('should get item by hash key', function(done) {
      User.get({ id : '123456789'}, function (err, acc) {
        expect(err).to.not.exist;
        expect(acc).to.exist;
        expect(acc.get()).to.have.keys(['id', 'email', 'age', 'roles', 'acceptedTerms', 'settings', 'things']);
        return done();
      });
    });

    it('should get return selected attributes AttributesToGet param', function(done) {
      User.get({ id : '123456789'},{AttributesToGet : ['email', 'age']}, function (err, acc) {
        expect(err).to.not.exist;
        expect(acc).to.exist;
        expect(acc.get()).to.have.keys(['email', 'age']);
        return done();
      });
    });

    it('should get return selected attributes using ProjectionExpression param', function(done) {
      User.get({ id : '123456789'},{ProjectionExpression : 'email, age, settings.nickname'}, function (err, acc) {
        expect(err).to.not.exist;
        expect(acc).to.exist;
        expect(acc.get()).to.have.keys(['email', 'age', 'settings']);
        expect(acc.get('settings').nickname).to.exist;
        return done();
      });
    });


  });

  describe('#update', function () {
    it('should update item appended role', function(done) {
      User.update({
        id : '123456789',
        roles  : {$add : 'tester'}
      }, function (err, acc) {
        expect(err).to.not.exist;
        expect(acc).to.exist;
        expect(acc.get()).to.have.keys(['id', 'email', 'age', 'roles', 'acceptedTerms', 'settings', 'things']);
        expect(acc.get('roles').sort()).to.eql(['tester', 'user']);

        return done();
      });
    });

    it('should remove name attribute from user record when set to empty string', function(done) {
      User.update({ id : '9999', name : ''}, function (err, acc) {
        expect(err).to.not.exist;
        expect(acc).to.exist;

        expect(acc.get()).to.have.keys(['id', 'email', 'age', 'roles', 'acceptedTerms']);
        return done();
      });
    });

    it('should update age using expected value', function(done) {
      User.update({ id : '9999', age : 100}, {expected: {age: 99}}, function (err, acc) {
        expect(err).to.not.exist;
        expect(acc).to.exist;

        expect(acc.get('age')).to.eql(100);
        return done();
      });
    });

    it('should update email using expected that an email already exists', function(done) {
      User.update({ id : '9999', email : 'new9999@test.com'}, {expected: {email: {Exists : true}}}, function (err, acc) {
        expect(err).to.not.exist;
        expect(acc).to.exist;

        expect(acc.get('email')).to.eql('new9999@test.com');
        return done();
      });
    });

    it('should remove settings attribute from user record', function(done) {
      User.update({ id : '123456789', settings : null}, function (err, acc) {
        expect(err).to.not.exist;
        expect(acc).to.exist;

        expect(acc.get()).to.have.keys(['id', 'email', 'age', 'roles', 'acceptedTerms', 'things']);
        return done();
      });
    });

    it('should update User using updateExpression', function(done) {
      var params = {};
      params.UpdateExpression = 'ADD #a :x SET things[0].buz = :y';
      params.ConditionExpression = '#a = :current';
      params.ExpressionAttributeNames = {'#a' : 'age'};
      params.ExpressionAttributeValues = {':x' : 1, ':y' : 22, ':current' : 30};

      User.update({ id : '123456789'}, params, function (err, acc) {
        expect(err).to.not.exist;
        expect(acc).to.exist;
        expect(acc.get('age')).to.equal(31);
        expect(acc.get('things')).to.eql([{fiz : 3, buz : 22, fizbuz: 35}]);
        return done();
      });
    });

    it('should update Movie using updateExpressions', function (done) {
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

      Movie.update({title : 'Movie 0', description : 'This is a description'}, params, function (err, mov) {
        expect(err).to.not.exist();

        expect(mov.get('description')).to.eql('This is a description');
        expect(mov.get('releaseYear')).to.eql(2002);
        expect(mov.get('updatedAt')).to.exist;
        return done();
      });
    });

    it('should update item with dynamic keys', function(done) {
      DynamicKeyModel.update({
        id : 'rand-5',
        color : 'green',
        settings : { email : 'dynupdate@test.com'}
      }, function (err, acc) {
        expect(err).to.not.exist;
        expect(acc).to.exist;
        expect(acc.get()).to.have.keys(['id', 'settings', 'color']);

        expect(acc.get()).to.eql( {
          id : 'rand-5',
          color : 'green',
          settings : { email : 'dynupdate@test.com'}
        });

        return done();
      });
    });

  });

  describe('#getItems', function () {
    it('should return 3 items', function(done) {
      User.getItems(['userid-1', 'userid-2', 'userid-3'], function (err, accounts) {
        expect(err).to.not.exist;
        expect(accounts).to.have.length(3);
        return done();
      });
    });

    it('should return 2 items with only selected attributes', function(done) {
      var opts = {AttributesToGet : ['email', 'age']};

      User.getItems(['userid-1', 'userid-2'], opts, function (err, accounts) {
        expect(err).to.not.exist;
        expect(accounts).to.have.length(2);
        _.each(accounts, function (acc) {
          expect(acc.get()).to.have.keys(['email', 'age']);
        });

        return done();
      });
    });
  });

  describe('#query', function () {
    it('should return users tweets', function(done) {
      Tweet.query('userid-1').exec(function (err, data) {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        _.each(data.Items, function (t) {
          expect(t.get('UserId')).to.eql('userid-1');
        });

        return done();
      });
    });

    it('should return users tweets with specific attributes', function(done) {
      Tweet.query('userid-1').attributes(['num', 'content']).exec(function (err, data) {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        _.each(data.Items, function (t) {
          expect(t.get('UserId')).to.not.exist;
          expect(t.get()).to.include.keys('num', 'content');
        });

        return done();
      });
    });

    it('should return tweets using secondaryIndex', function(done) {
      Tweet.query('userid-1')
      .usingIndex('PublishedDateTimeIndex')
      .consistentRead(true)
      .descending()
      .exec(function (err, data) {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        var prev;
        _.each(data.Items, function (t) {
          expect(t.get('UserId')).to.eql('userid-1');

          var published = t.get('PublishedDateTime');

          if(prev) {
            expect(published).to.be.at.most(prev);
          }

          prev = published;
        });

        return done();
      });
    });

    it('should return tweets using secondaryIndex and date object', function(done) {
      var oneMinAgo = new Date(new Date().getTime() - 60*1000);

      Tweet.query('userid-1')
      .usingIndex('PublishedDateTimeIndex')
      .where('PublishedDateTime').gt(oneMinAgo)
      .descending()
      .exec(function (err, data) {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        var prev;
        _.each(data.Items, function (t) {
          expect(t.get('UserId')).to.eql('userid-1');

          var published = t.get('PublishedDateTime');

          if(prev) {
            expect(published).to.be.at.most(prev);
          }

          prev = published;
        });

        return done();
      });
    });

    it('should return tweets that match filters', function(done) {
      Tweet.query('userid-1')
      .filter('num').between(4, 8)
      .filter('tag').exists()
      .exec(function (err, data) {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        _.each(data.Items, function (t) {
          expect(t.get('UserId')).to.eql('userid-1');
          expect(t.get('num')).to.be.above(3);
          expect(t.get('num')).to.be.below(9);
          expect(t.get('tag')).to.exist();
        });

        return done();
      });
    });

    it('should return tweets that match exists filter', function(done) {
      Tweet.query('userid-1')
        .filter('tag').exists()
        .exec(function (err, data) {
          expect(err).to.not.exist;
          expect(data.Items).to.have.length.above(0);

          _.each(data.Items, function (t) {
            expect(t.get('UserId')).to.eql('userid-1');
            expect(t.get('tag')).to.exist();
          });

          return done();
        });
    });

    it('should return tweets that match IN filter', function(done) {
      Tweet.query('userid-1')
        .filter('num').in([4, 6, 8])
        .exec(function (err, data) {
          expect(err).to.not.exist;
          expect(data.Items).to.have.length.above(0);

          _.each(data.Items, function (t) {
            expect(t.get('UserId')).to.eql('userid-1');
            var c = _.contains([4, 6, 8], t.get('num'));
            expect(c).to.be.true;
          });

          return done();
        });
    });

    it('should return tweets that match expression filters', function(done) {
      Tweet.query('userid-1')
      .filterExpression('#num BETWEEN :low AND :high AND attribute_exists(#tag)')
      .expressionAttributeValues({ ':low' : 4, ':high' : 8})
      .expressionAttributeNames({ '#num' : 'num', '#tag' : 'tag'})
      .exec(function (err, data) {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        _.each(data.Items, function (t) {
          expect(t.get('UserId')).to.eql('userid-1');
          expect(t.get('num')).to.be.above(3);
          expect(t.get('num')).to.be.below(9);
          expect(t.get('tag')).to.exist();
        });

        return done();
      });
    });

    it('should return tweets with projection expression', function(done) {
      Tweet.query('userid-1')
      .projectionExpression('#con, UserId')
      .expressionAttributeNames({ '#con' : 'content'})
      .exec(function (err, data) {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        _.each(data.Items, function (t) {
          expect(t.get()).to.have.keys(['content', 'UserId']);
        });

        return done();
      });
    });

    it('should return all tweets from user', function(done) {
      Tweet.query('userid-1').limit(2).loadAll().exec(function (err, data) {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        _.each(data.Items, function (t) {
          expect(t.get('UserId')).to.eql('userid-1');
        });

        return done();
      });
    });

  });


  describe('#scan', function () {
    it('should return all users', function(done) {
      User.scan().loadAll().exec(function (err, data) {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        return done();
      });
    });

    it('should return all users with limit', function(done) {
      User.scan().limit(2).loadAll().exec(function (err, data) {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        return done();
      });
    });

    it('should return users with specific attributes', function(done) {
      User.scan()
        .where('age').gt(18)
        .attributes(['email', 'roles', 'age']).exec(function (err, data) {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);
        _.each(data.Items, function (u) {
          expect(u.get()).to.include.keys('email', 'roles', 'age');
        });

        return done();
      });
    });

    it('should return 10 users', function(done) {
      User.scan().limit(10).exec(function (err, data) {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length(10);

        return done();
      });
    });

    it('should return users older than 18', function(done) {
      User.scan()
      .where('age').gt(18)
      .exec(function (err, data) {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        _.each(data.Items, function (u) {
          expect(u.get('age')).to.be.above(18);
        });

        return done();
      });
    });

    it('should return users matching multiple filters', function(done) {
      User.scan()
      .where('age').between(18, 22)
      .where('email').beginsWith('test1')
      .exec(function (err, data) {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        _.each(data.Items, function (u) {
          expect(u.get('age')).to.be.within(18, 22);
          expect(u.get('email')).to.match(/^test1.*/);
        });

        return done();
      });
    });

    it('should return users contains admin role', function(done) {
      User.scan()
      .where('roles').contains('admin')
      .exec(function (err, data) {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        _.each(data.Items, function (u) {
          expect(u.get('roles')).to.include('admin');
        });

        return done();
      });
    });

    it('should return users using stream interface', function(done) {
      var stream = User.scan().exec();

      var called = false;
      stream.on('readable', function () {
        called = true;
        var data = stream.read();
        if(data) {
          expect(data.Items).to.have.length.above(0);
        }
      });

      stream.on('end', function () {
        expect(called).to.be.true;
        return done();
      });
    });

    it('should return users that match expression filters', function(done) {
      User.scan()
      .filterExpression('#age BETWEEN :low AND :high AND begins_with(#email, :e)')
      .expressionAttributeValues({ ':low' : 18, ':high' : 22, ':e' : 'test1'})
      .expressionAttributeNames({ '#age' : 'age', '#email' : 'email'})
      .exec(function (err, data) {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        _.each(data.Items, function (u) {
          expect(u.get('age')).to.be.within(18, 22);
          expect(u.get('email')).to.match(/^test1.*/);
        });

        return done();
      });
    });

    it('should return users between ages', function(done) {
      User.scan()
        .where('age').between(18, 22)
        .where('email').beginsWith('test1')
        .exec(function (err, data) {
          expect(err).to.not.exist;
          expect(data.Items).to.have.length.above(0);

          _.each(data.Items, function (u) {
            expect(u.get('age')).to.be.within(18, 22);
            expect(u.get('email')).to.match(/^test1.*/);
          });

          return done();
        });
    });

    it('should return users matching IN filter', function(done) {
      User.scan()
        .where('age').in([2, 9, 20])
        .exec(function (err, data) {
          expect(err).to.not.exist;
          expect(data.Items).to.have.length.above(0);

          _.each(data.Items, function (u) {
            var c = _.contains([2, 9, 20], u.get('age'));
            expect(c).to.be.true;
          });

          return done();
        });
    });

    it('should return users with projection expression', function(done) {
      User.scan()
      .projectionExpression('age, email, #roles')
      .expressionAttributeNames({ '#roles' : 'roles'})
      .exec(function (err, data) {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        _.each(data.Items, function (u) {
          expect(u.get()).to.have.keys(['age', 'email', 'roles']);
        });

        return done();
      });
    });

    it('should load all users with limit', function(done) {
      User.scan().loadAll().limit(2).exec(function (err, data) {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        return done();
      });
    });

    it('should return users using stream interface and limit', function(done) {
      var stream = User.scan().loadAll().limit(2).exec();

      var called = false;
      stream.on('readable', function () {
        called = true;
        var data = stream.read();

        if(data) {
          expect(data.Items).to.have.length.within(0, 2);
        }
      });

      stream.on('end', function () {
        expect(called).to.be.true;
        return done();
      });
    });

    it('should load tweets using not null tag clause', function(done) {
      Tweet.scan().where('tag').notNull().exec(function (err, data) {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        _.each(data.Items, function (t) {
          expect(t.get('tag')).to.exist;
        });

        return done();
      });
    });

  });

  describe('#parallelScan', function () {
    it('should return all users', function(done) {
      User.parallelScan(4).exec(function (err, data) {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        return done();
      });

    });

    it('should return users older than 18', function(done) {
      User.parallelScan(4)
      .where('age').gt(18)
      .exec(function (err, data) {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        _.each(data.Items, function (u) {
          expect(u.get('age')).to.be.above(18);
        });

        return done();
      });
    });

    it('should return users using stream interface', function(done) {
      var stream = User.parallelScan(4).exec();

      var called = false;
      stream.on('readable', function () {
        called = true;
        var data = stream.read();

        if(data) {
          expect(data.Items).to.have.length.above(0);
        }
      });

      stream.on('end', function () {
        expect(called).to.be.true;
        return done();
      });
    });

  });


  describe('timestamps', function () {
    var Model;
    var ModelCustomTimestamps;

    before(function (done) {
      Model = vogels.define('vogels-int-test-timestamp', {
        hashKey : 'id',
        timestamps : true,
        schema : {
          id : Joi.string()
        }
      });

      ModelCustomTimestamps = vogels.define('vogels-int-test-timestamp-custom', {
        hashKey : 'id',
        timestamps : true,
        createdAt : 'created',
        updatedAt : 'updated',
        schema : {
          id : Joi.string()
        }
      });


      return vogels.createTables(done);
    });

    it('should add createdAt param', function (done) {
      Model.create({id : 'test-1'}, function (err) {
        expect(err).to.not.exist;

        Model.get('test-1', function (err2, data) {
          expect(err2).to.not.exist;

          expect(data.get('id')).to.eql('test-1');
          expect(data.get('createdAt')).to.exist;

          return done();
        });

      });
    });

    it('should add updatedAt param', function (done) {
      Model.update({id : 'test-2'}, function (err) {
        expect(err).to.not.exist;

        Model.get('test-2', function (err2, data) {
          expect(err2).to.not.exist;

          expect(data.get('id')).to.eql('test-2');
          expect(data.get('updatedAt')).to.exist;

          return done();
        });

      });
    });

    it('should add custom createdAt param', function (done) {
      ModelCustomTimestamps.create({id : 'test-1'}, function (err) {
        expect(err).to.not.exist;

        ModelCustomTimestamps.get('test-1', function (err2, data) {
          expect(err2).to.not.exist;

          expect(data.get('id')).to.eql('test-1');
          expect(data.get('created')).to.exist;

          return done();
        });
      });
    });

    it('should add custom updatedAt param', function (done) {
      ModelCustomTimestamps.update({id : 'test-2'}, function (err) {
        expect(err).to.not.exist;

        ModelCustomTimestamps.get('test-2', function (err2, data) {
          expect(err2).to.not.exist;

          expect(data.get('id')).to.eql('test-2');
          expect(data.get('updated')).to.exist;

          return done();
        });

      });
    });

  });

  describe('#destroy', function () {
    var userId;
    beforeEach(function (done) {
      User.create({email : 'destroy@test.com', age : 20, roles : ['tester']}, function (err, acc) {
        expect(err).to.not.exist;
        userId = acc.get('id');

        return done();
      });
    });

    it('should destroy item with hash key', function(done) {
      User.destroy({ id : userId }, function (err) {
        expect(err).to.not.exist;
        return done();
      });
    });

    it('should destroy item and return old values', function(done) {
      User.destroy({ id : userId }, {ReturnValues : 'ALL_OLD'}, function (err, acc) {
        expect(err).to.not.exist;
        expect(acc).to.exist;
        expect(acc.get('email')).to.eql('destroy@test.com');
        return done();
      });
    });

    it('should return condition exception when using ConditionExpression', function(done) {
      var params = {};
      params.ConditionExpression = '#i = :x';
      params.ExpressionAttributeNames = {'#i' : 'id'};
      params.ExpressionAttributeValues = {':x' : 'dontexist'};

      User.destroy({id : 'dontexist'}, params, function (err, acc) {
        expect(err).to.exist;
        expect(err.code).to.eql('ConditionalCheckFailedException');
        expect(acc).to.not.exist;

        return done();
      });
    });

    it('should return condition exception when using Expected shorthand', function(done) {
      var opts = {expected : {id : 'dontexist'}};

      User.destroy({id : 'dontexist'}, opts, function (err, acc) {
        expect(err).to.exist;
        expect(err.code).to.eql('ConditionalCheckFailedException');
        expect(acc).to.not.exist;

        return done();
      });
    });

  });


  describe('model methods', function () {

    it('#save with passed in attributes', function (done) {
      var t = new Tweet({
        UserId : 'tester-1',
        content : 'save test tweet',
        tag : 'test'
      });

      t.save(function (err) {
        expect(err).to.not.exist;
        return done();
      });
    });

    it('#save without passed in attributes', function (done) {
      var t = new Tweet();

      var attrs = { UserId : 'tester-1', content : 'save test tweet', tag : 'test' };
      t.set(attrs);

      t.save(function (err) {
        expect(err).to.not.exist;
        return done();
      });
    });

    it('#save without callback', function (done) {
      var t = new Tweet({
        UserId : 'tester-1',
        content : 'save test tweet',
        tag : 'test'
      });

      t.save();

      return done();
    });

    it('#update with callback', function (done) {
      Tweet.create({UserId : 'tester-2', content : 'update test tweet'}, function (err, tweet) {
        expect(err).to.not.exist;

        tweet.set({tag : 'update'});

        tweet.update(function (err) {
          expect(err).to.not.exist;
          expect(tweet.get('tag')).to.eql('update');
          return done();
        });

      });
    });

    it('#update without callback', function (done) {
      Tweet.create({UserId : 'tester-2', content : 'update test tweet'}, function (err, tweet) {
        expect(err).to.not.exist;

        tweet.set({tag : 'update'});

        tweet.update();

        return done();
      });
    });


    it('#destroy with callback', function (done) {
      Tweet.create({UserId : 'tester-2', content : 'update test tweet'}, function (err, tweet) {
        expect(err).to.not.exist;

        tweet.destroy(function (err) {
          expect(err).to.not.exist;
          return done();
        });
      });
    });

    it('#destroy without callback', function (done) {
      Tweet.create({UserId : 'tester-2', content : 'update test tweet'}, function (err, tweet) {
        expect(err).to.not.exist;

        tweet.destroy();

        return done();
      });
    });

    it('#toJSON', function (done) {
      Tweet.create({UserId : 'tester-2', content : 'update test tweet'}, function (err, tweet) {
        expect(err).to.not.exist;

        expect(tweet.toJSON()).to.have.keys(['UserId', 'content', 'TweetID', 'PublishedDateTime']);
        return done();
      });
    });
  });

});
