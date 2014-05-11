'use strict';

var _            = require('lodash'),
    Item         = require('./item'),
    Query        = require('./query'),
    Scan         = require('./scan'),
    EventEmitter = require('events').EventEmitter,
    async        = require('async'),
    utils        = require('./utils'),
    ParallelScan = require('./parallelScan');

var internals = {};

var Table = module.exports = function (name, schema, serializer, dynamodb) {
  this.config = {name : name};
  this.schema = schema;
  this.serializer = serializer;
  this.dynamodb = dynamodb;

  this._before = new EventEmitter();
  this.before = this._before.on.bind(this._before);

  this._after= new EventEmitter();
  this.after = this._after.on.bind(this._after);
};

Table.prototype.initItem = function (attrs) {
  var self = this;

  if(self.itemFactory) {
    return new self.itemFactory(attrs);
  } else {
    return new Item(attrs, self);
  }
};

Table.prototype.tableName = function () {
  if(this.schema.tableName) {
    if(_.isFunction(this.schema.tableName)) {
      return this.schema.tableName.call(this);
    } else {
      return this.schema.tableName;
    }
  } else {
    return this.config.name;
  }
};

Table.prototype.get = function (hashKey, rangeKey, options, callback) {
  var self = this;

  if (_.isPlainObject(rangeKey) && typeof options === 'function' && !callback) {
    callback = options;
    options = rangeKey;
    rangeKey = null;
  } else if (typeof rangeKey === 'function' && !callback) {
    callback = rangeKey;
    options = {};
    rangeKey = null;
  } else if (_.isPlainObject(rangeKey) && !callback) {
    callback = options;
    options = rangeKey;
    rangeKey = null;
  } else if (typeof options === 'function' && !callback) {
    callback = options;
    options = {};
  }

  var params = {
    TableName : self.tableName(),
    Key : self.serializer.buildKey(hashKey, rangeKey, self.schema)
  };

  params = _.merge({}, params, options);

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

internals.callBeforeHooks = function (table, name, startFun, callback) {
  var listeners = table._before.listeners(name);

  return async.waterfall([startFun].concat(listeners), callback);
};

Table.prototype.create = function (item, options, callback) {
  var self = this;

  if (typeof options === 'function' && !callback) {
    callback = options;
    options = {};
  }

  callback = callback || function () {};

  var start = function (callback) {
    var data = self.schema.applyDefaults(item);
    return callback(null, data);
  };

  internals.callBeforeHooks(self, 'create', start, function (err, data) {
    if(err) {
      return callback(err);
    }

    var validationError = self.schema.validate(data);

    if(validationError) {
      return callback(validationError);
    }

    var attrs = utils.omitNulls(data);

    var params = {
      TableName : self.tableName(),
      Item : self.serializer.serializeItem(self.schema, attrs)
    };

    if (options.expected) {
      params.Expected = self.serializer.serializeItem(self.schema, options.expected, {expected : true});
    }

    self.dynamodb.putItem(params, function (err) {
      if(err) {
        return callback(err);
      }

      var item = self.initItem(attrs);
      self._after.emit('create', item);

      return callback(null, item);
    });
  });
};

Table.prototype.update = function (item, options, callback) {
  var self = this;

  if (typeof options === 'function' && !callback) {
    callback = options;
    options = {};
  }

  callback = callback || function () {};

  var start = function (callback) {
    return callback(null, item);
  };

  internals.callBeforeHooks(self, 'update', start, function (err, data) {
    if(err) {
      return callback(err);
    }

    var hashKey = data[self.schema.hashKey];
    var rangeKey = data[self.schema.rangeKey] || null;

    var params = {
      TableName : self.tableName(),
      Key : self.serializer.buildKey(hashKey, rangeKey, self.schema),
      AttributeUpdates : self.serializer.serializeItemForUpdate(self.schema, 'PUT', data),
      ReturnValues : 'ALL_NEW'
    };

    if (options.expected) {
      options.Expected = self.serializer.serializeItem(self.schema, options.expected, {expected : true});

      delete options.expected;
    }

    params = _.merge({}, params, options);

    self.dynamodb.updateItem(params, function (err, data) {
      if(err) {
        return callback(err);
      }

      var result = null;
      if(data.Attributes) {
        result = self.initItem(self.serializer.deserializeItem(self.schema, data.Attributes));
      }

      self._after.emit('update', result);
      return callback(null, result);
    });

  });
};

Table.prototype.destroy = function (hashKey, rangeKey, options, callback) {
  var self = this;

  if (_.isPlainObject(rangeKey) && typeof options === 'function' && !callback) {
    callback = options;
    options = rangeKey;
    rangeKey = null;
  } else if (typeof rangeKey === 'function' && !callback) {
    callback = rangeKey;
    options = {};
    rangeKey = null;
  } else if (_.isPlainObject(rangeKey) && !callback) {
    callback = options;
    options = rangeKey;
    rangeKey = null;
  } else if (typeof options === 'function' && !callback) {
    callback = options;
    options = {};
  }

  callback = callback || function () {};

  if (_.isPlainObject(hashKey)) {
    rangeKey = hashKey[self.schema.rangeKey] || null;
    hashKey = hashKey[self.schema.hashKey];
  }

  var params = {
    TableName : self.tableName(),
    Key : self.serializer.buildKey(hashKey, rangeKey, self.schema)
  };

  if (options.expected) {
    options.Expected = self.serializer.serializeItem(self.schema, options.expected, {expected : true});

    delete options.expected;
  }

  params = _.merge({}, params, options);

  self.dynamodb.deleteItem(params, function (err, data) {
    if(err) {
      return callback(err);
    }

    var item = null;
    if(data.Attributes) {
      item = self.initItem(self.serializer.deserializeItem(self.schema, data.Attributes));
    }

    self._after.emit('destroy', item);
    return callback(null, item);
  });
};

Table.prototype.query = function (hashKey) {
  var self = this;

  return new Query(hashKey, self, self.serializer);
};

Table.prototype.scan = function () {
  var self = this;

  return new Scan(self, self.serializer);
};

Table.prototype.parallelScan= function (totalSegments) {
  var self = this;

  return new ParallelScan(self, self.serializer, totalSegments);
};


internals.deserializeItems = function (table, callback) {
  return function (err, data) {
    if(err) {
      return callback(err);
    }

    var result = {};
    if(data.Items) {
      result.Items = _.map(data.Items, function(i) {
        return table.initItem(table.serializer.deserializeItem(table.schema, i));
      });

      delete data.Items;
    }

    if(data.LastEvaluatedKey) {
      result.LastEvaluatedKey = table.serializer.deserializeKeys(table.schema, data.LastEvaluatedKey);

      delete data.LastEvaluatedKey;
    }

    return callback(null, _.merge({}, data, result));
  };

};

Table.prototype.runQuery = function(params, callback) {
  var self = this;

  self.dynamodb.query(params, internals.deserializeItems(self, callback));
};

Table.prototype.runScan = function(params, callback) {
  var self = this;

  self.dynamodb.scan(params, internals.deserializeItems(self, callback));
};

Table.prototype.runBatchGetItems = function (params, callback) {

  var self = this;
  self.dynamodb.batchGetItem(params, callback);
};

internals.attributeDefinition = function (schema, key) {
  return {
    AttributeName : key,
    AttributeType : schema.attrs[key].dynamoType
  };
};

internals.keySchema = function (hashKey, rangeKey) {
  var result = [{
    AttributeName : hashKey,
    KeyType : 'HASH'
  }];

  if(rangeKey) {
    result.push({
      AttributeName : rangeKey,
      KeyType : 'RANGE'
    });
  }

  return result;
};

internals.secondaryIndex = function (schema, indexKey) {
  var indexName = indexKey + 'Index';

  return {
    IndexName : indexName,
    KeySchema : internals.keySchema(schema.hashKey, indexKey),
    Projection : {
      ProjectionType : 'ALL'
    }
  };
};

internals.globalIndex = function (indexName, params) {
  var projection = params.Projection || { ProjectionType : 'ALL' };

  return {
    IndexName : indexName,
    KeySchema : internals.keySchema(params.hashKey, params.rangeKey),
    Projection : projection,
    ProvisionedThroughput: {
      ReadCapacityUnits: params.readCapacity || 1,
      WriteCapacityUnits: params.writeCapacity || 1
    }
  };
};

Table.prototype.createTable = function (options, callback) {
  var self = this;

  if (typeof options === 'function' && !callback) {
    callback = options;
    options = {};
  }
  var attributeDefinitions = [];

  attributeDefinitions.push(internals.attributeDefinition(self.schema, self.schema.hashKey));

  if(self.schema.rangeKey) {
    attributeDefinitions.push(internals.attributeDefinition(self.schema, self.schema.rangeKey));
  }

  var localSecondaryIndexes = [];

  _.forEach(self.schema.secondaryIndexes, function (key) {
    attributeDefinitions.push(internals.attributeDefinition(self.schema, key));
    localSecondaryIndexes.push(internals.secondaryIndex(self.schema, key));
  });

  var globalSecondaryIndexes = [];

  _.forEach(self.schema.globalIndexes, function (params, indexName) {

    if(!_.find(attributeDefinitions, { 'AttributeName': params.hashKey })) {
      attributeDefinitions.push(internals.attributeDefinition(self.schema, params.hashKey));
    }

    if(params.rangeKey && !_.find(attributeDefinitions, { 'AttributeName': params.rangeKey })) {
      attributeDefinitions.push(internals.attributeDefinition(self.schema, params.rangeKey));
    }

    globalSecondaryIndexes.push(internals.globalIndex(indexName, params));
  });

  var keySchema = internals.keySchema(self.schema.hashKey, self.schema.rangeKey);

  var params = {
    AttributeDefinitions : attributeDefinitions,
    TableName : self.tableName(),
    KeySchema : keySchema,
    ProvisionedThroughput : {
      ReadCapacityUnits : options.readCapacity || 1,
      WriteCapacityUnits : options.writeCapacity || 1
    }
  };

  if(localSecondaryIndexes.length >= 1) {
    params.LocalSecondaryIndexes = localSecondaryIndexes;
  }

  if(globalSecondaryIndexes.length >= 1) {
    params.GlobalSecondaryIndexes = globalSecondaryIndexes;
  }

  self.dynamodb.createTable(params, callback);
};

Table.prototype.describeTable = function (callback) {

  var params = {
    TableName : this.tableName(),
  };

  this.dynamodb.describeTable(params, callback);
};

Table.prototype.updateTable = function (throughput, callback) {
  callback = callback || function () {};

  var params = {
    TableName : this.tableName(),
    ProvisionedThroughput : {
      ReadCapacityUnits : throughput.readCapacity,
      WriteCapacityUnits : throughput.writeCapacity
    }
  };

  this.dynamodb.updateTable(params, callback);
};
