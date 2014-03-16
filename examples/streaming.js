'use strict';

var vogels = require('../index'),
    AWS    = vogels.AWS;

AWS.config.loadFromPath(process.env.HOME + '/.ec2/credentials.json');

var Product = vogels.define('Product', function (schema) {
  schema.String('ProductId', {hashKey: true});
  schema.String('host');
  schema.String('url');
  schema.String('title');

  schema.Date('created');
});

var printStream = function (msg, stream) {
  var count = 0;
  stream.on('error', function (err) {
    console.log('error ' + msg, err);
  });

  stream.on('readable', function () {
    count++;
    console.log('----------------------' + count + '--------------------------');
    console.log('Scanned ' + stream.read().Count + ' products - ' + msg);
  });

  stream.on('end', function () {
    console.log('-------------------------------------------------');
    console.log('Finished ' + msg);
    console.log('-------------------------------------------------');
  });
};

var s1 = Product.scan().loadAll().exec();
printStream('Loading All Accounts', s1);

var s2 = Product.scan().limit(100).loadAll().exec();
printStream('Load All Accounts 100 at a time', s2);

var totalSegments = 4;
var s3 = Product.parallelScan(totalSegments)
  .attributes('url')
  .exec();

printStream('Parallel Loaded urls', s3);
