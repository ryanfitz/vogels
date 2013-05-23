'use strict';

var vogels = require('../index'),
    AWS    = vogels.AWS;

AWS.config.loadFromPath(process.env.HOME + '/.ec2/credentials.json');

var Product = vogels.define('Product', function (schema) {
  schema.String('id', {hashKey: true});
  schema.Number('accountID');
  schema.String('purchased');
  schema.Date('ctime');
  schema.Number('price');
});

var printInfo = function (err, resp) {
  if(err) {
    console.log(err);
    return;
  }

  console.log('Count', resp.Count);
  console.log('Scanned Count', resp.ScannedCount);

  var totalPrices = resp.Items.reduce(function (total, item) {
    return total += item.get('price');
  }, 0);

  console.log('Total purchased', totalPrices);
  console.log('Average purchased price', totalPrices / resp.Count);
};

var totalSegments = 8;

Product.parallelScan(totalSegments)
  .where('purchased').equals('true')
  .attributes('price')
  .exec(printInfo);
