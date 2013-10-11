'use strict';

var Item   = require('../lib/item'),
    Table  = require('../lib/table'),
    Schema = require('../lib/schema'),
    chai   = require('chai'),
    helper = require('./test-helper');

chai.should();

describe('item', function() {
  it('JSON.stringify should only serialize attrs', function() {
    var schema = new Schema();
    schema.Number('num');
    schema.String('name');

    var table = new Table('mockTable', schema, helper.mockSerializer(), helper.mockDynamoDB());
    var attrs = {num: 1, name: 'foo'};
    var item = new Item(attrs, table);
    var stringified = JSON.stringify(item);

    stringified.should.equal(JSON.stringify(attrs));
  });
});
