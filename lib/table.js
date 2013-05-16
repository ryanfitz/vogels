'use strict';

var Item = require('./item');

var Table = module.exports = function (name, schema, serializer, dynamodb) {
  this.config = {name : name};
  this.schema = schema;
  this.serializer = serializer;
  this.dynamodb = dynamodb;
};

Table.prototype.initItem = function (attrs) {
  var self = this;

  if(self.itemFactory) {
    return new self.itemFactory(attrs);
  } else {
    return new Item(attrs, self);
  }
};

Table.prototype.get = function (hashKey, rangeKey, callback) {
  var self = this;

  if (typeof rangeKey === 'function' && !callback) {
    callback = rangeKey;
    rangeKey = null;
  }

  var params = {
    TableName : self.config.name,
    Key : self.serializer.buildKey(hashKey, rangeKey, self.schema)
  };

  self.dynamodb.getItem(params, function (err, data) {
    if(err) {
      return callback(err);
    }

    var item = null;
    if(data.Item) {
      item = self.initItem(self.serializer.deserializeItem(self.schema, data.Item));
    }

    return callback(null, item);
  });
};

Table.prototype.create = function (item, callback) {
  var self = this;

  var params = {
    TableName : self.config.name,
    Item : self.serializer.serializeItem(self.schema, item)
  };

  self.dynamodb.putItem(params, function (err) {
    if(err) {
      return callback(err);
    }

    return callback(null, self.initItem(item));
  });
};

Table.prototype.update = function (item, callback) {
  var self = this;

  var params = {
    TableName : self.config.name,
    Key : self.serializer.buildKey(item, null, self.schema),
    AttributeUpdates : self.serializer.serializeItemForUpdate(self.schema, 'PUT', item),
    ReturnValues : 'ALL_NEW'
  };

  self.dynamodb.updateItem(params, function (err, data) {
    if(err) {
      return callback(err);
    }

    var item = null;
    if(data.Attributes) {
      item = self.initItem(self.serializer.deserializeItem(self.schema, data.Attributes));
    }

    return callback(null, item);
  });
};

Table.prototype.destroy = function (hashKey, rangeKey, callback) {
  var self = this;

  if (typeof rangeKey === 'function' && !callback) {
    callback = rangeKey;
    rangeKey = null;
  }

  var params = {
    TableName : self.config.name,
    Key : self.serializer.buildKey(hashKey, rangeKey, self.schema)
  };

  self.dynamodb.deleteItem(params, function (err) {
    if(err) {
      return callback(err);
    }

    return callback();
  });
};


