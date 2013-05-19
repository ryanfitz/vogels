'use strict';

var vogels = require('vogels');

var Account = vogels.define('Account', function (schema) {
  schema.String('email', {hashKey: true});
  schema.String('username').alphanum().min(3).max(30).required();
  schema.String('name').required();
  schema.Number('birthYear').min(1850).max(2012);
});

var BlogPost = vogels.define('BlogPost', function (schema) {
  schema.String('email', {hashKey: true});
  schema.String('title', {rangeKey: true});
  schema.Date('published', {secondaryIndex: true});
  schema.String('content').required();
});

var acc = new Account({
  email : 'vogels@test.com',
  username : 'werner',
  name : 'Werner Vogels',
  birthYear: 1958
});

var publishedPost = new BlogPost({
  email : acc.email,
  title : 'Expanding the Cloud: Faster, More Flexible Queries with DynamoDB',
  content : 'Today, I’m thrilled to announce that we have expanded the query capabilities of DynamoDB.',
  published : new Date('2013-04-17 10:30:00')
});


var unpublishedPost = new BlogPost({
  email : acc.email,
  title : 'Expanding the Cloud: Spaceships!',
  content : 'Today, I’m thrilled to announce...'
});

acc.save(function (err){
  if(err) {console.log('error saving account', err);}

  // account created!
  //save both posts

  unpublishedPost.save();
  publishedPost.save();
});

Account.get('vogels@test.com', function (err, account) {
  if(err) {console.log('error getting account', err);}

  // got account
  console.log('Found account', account);
});

BlogPost.query('vogels@test.com').exec(console.log); // Get all blog posts created by vogels
BlogPost.query('vogels@test.com').usingIndex('published').exec(console.log); // Get all published blogposts by vogels
