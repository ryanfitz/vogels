'use strict';

var Item   = require('../lib/item'),
    Table  = require('../lib/table'),
    Schema = require('../lib/schema'),
    chai   = require('chai'),
    helper = require('./test-helper');

chai.should();
var expect = chai.expect;

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
    
  it('should not merge an object', function() {
      var schema = new Schema();
      schema.JSON('json');
      
      var table = new Table('mockTable', schema, helper.mockSerializer(), helper.mockDynamoDB());
      var attrs = {
          json: {
              stuff: "foo",
              moreStuff: "bar"
         }
      };
      
      var newJson = {
              moreStuff: "baz"
      };
      
      var item = new Item(attrs, table);
      item.set({json: newJson});
      var check = item.get('json');
      expect(check.stuff).to.not.exist;
      check.moreStuff.should.equal("baz");
      
  });
});
