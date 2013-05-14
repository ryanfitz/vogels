'use strict';

var util = require('util'),
    events = require('events');

var Item = module.exports = function (attrs, table) {
  events.EventEmitter.call(this);

  this.attrs = attrs;
  this.table = table;
};

util.inherits(Item, events.EventEmitter);
