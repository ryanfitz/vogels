'use strict';

var _            = require('lodash'),
    Item         = require('./item'),
    Query        = require('./query'),
    Scan         = require('./scan'),
    EventEmitter = require('events').EventEmitter,
    async        = require('async'),
    utils        = require('./utils'),
    ParallelScan = require('./parallelScan'),
    expressions  = require('./expressions');

var internals = {};

var Table = module.exports = function (name, schema, serializer, docClient) {
  this.config = {name : name};
  this.schema = schema;
  this.serializer = serializer;
  this.docClient = docClient;

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
  } else if (typeof options === 'function' && !callback) {
    callback = options;
    options = {};
  }

  var params = {
    TableName : self.tableName(),
    Key : self.serializer.buildKey(hashKey, rangeKey, self.schema)
  };

  params = _.merge({}, params, options);

  self.docClient.getItem(params, function (err, data) {
    if(err) {
      return callback(err);
    }

    var item = null;
    if(data.Item) {
      item = self.initItem(self.serializer.deserializeItem(data.Item));
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

  callback = callback || _.noop;
  options = options || {};

  var start = function (callback) {
    var data = self.schema.applyDefaults(item);

    var paramName = _.isString(self.schema.createdAt) ? self.schema.createdAt : 'createdAt';

    if(self.schema.timestamps && self.schema.createdAt !== false && !_.has(data, paramName)) {
      data[paramName] = Date.now();
    }

    return callback(null, data);
  };

  internals.callBeforeHooks(self, 'create', start, function (err, data) {
    if(err) {
      return callback(err);
    }

    var result = self.schema.validate(data);

    if(result.error) {
      return callback(result.error);
    }

    var attrs = utils.omitNulls(data);

    var params = {
      TableName : self.tableName(),
      Item : self.serializer.serializeItem(self.schema, attrs)
    };

    if (options.expected) {
      params.Expected = self.serializer.serializeItem(self.schema, options.expected, {expected : true});
      options = _.omit(options, 'expected');
    }

    params = _.merge({}, params, options);

    self.docClient.putItem(params, function (err) {
      if(err) {
        return callback(err);
      }

      var item = self.initItem(attrs);
      self._after.emit('create', item);

      return callback(null, item);
    });
  });
};

internals.updateExpressions = function (schema, data, options) {
  var exp = expressions.serializeUpdateExpression(schema, data);

  if(options.UpdateExpression) {
    var parsed = expressions.parse(options.UpdateExpression);

    exp.expressions = _.reduce(parsed, function (result, val, key) {
      if(!_.isEmpty(val)) {
        result[key] = result[key].concat(val);
      }

      return result;
    }, exp.expressions);
  }

  if(_.isPlainObject(options.ExpressionAttributeValues)) {
    exp.values = _.merge({}, exp.values, options.ExpressionAttributeValues);
  }

  if(_.isPlainObject(options.ExpressionAttributeNames)) {
    exp.attributeNames = _.merge({}, exp.attributeNames, options.ExpressionAttributeNames);
  }

  return _.merge({}, {
    ExpressionAttributeValues : exp.values,
    ExpressionAttributeNames : exp.attributeNames,
    UpdateExpression : expressions.stringify(exp.expressions),
  });
};

Table.prototype.update = function (item, options, callback) {
  var self = this;

  if (typeof options === 'function' && !callback) {
    callback = options;
    options = {};
  }

  callback = callback || _.noop;
  options = options || {};

  var start = function (callback) {
    var paramName = _.isString(self.schema.updatedAt) ? self.schema.updatedAt : 'updatedAt';

    if(self.schema.timestamps && self.schema.updatedAt !== false && !_.has(item, paramName)) {
      item[paramName] = Date.now();
    }

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
      ReturnValues : 'ALL_NEW'
    };

	for(var key in data){
		if (data[key].$add === 0){
			delete data[key];
		}
	}

    var exp = internals.updateExpressions(self.schema, data, options);

    if(exp.UpdateExpression) {
      params.UpdateExpression = exp.UpdateExpression;
      delete options.UpdateExpression;
    }

    if(exp.ExpressionAttributeValues) {
      params.ExpressionAttributeValues = exp.ExpressionAttributeValues;
      delete options.ExpressionAttributeValues;
    }

    if(exp.ExpressionAttributeNames) {
      params.ExpressionAttributeNames = exp.ExpressionAttributeNames;
      delete options.ExpressionAttributeNames;
    }

    if (options.expected) {
      options.Expected = self.serializer.serializeItem(self.schema, options.expected, {expected : true});

      delete options.expected;
    }

    params = _.chain({}).merge(params, options).omit(_.isEmpty).value();

    self.docClient.updateItem(params, function (err, data) {
      if(err) {
        return callback(err);
      }

      var result = null;
      if(data.Attributes) {
        result = self.initItem(self.serializer.deserializeItem(data.Attributes));
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

  callback = callback || _.noop;
  options = options || {};

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

  self.docClient.deleteItem(params, function (err, data) {
    if(err) {
      return callback(err);
    }

    var item = null;
    if(data.Attributes) {
      item = self.initItem(self.serializer.deserializeItem(data.Attributes));
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
        return table.initItem(table.serializer.deserializeItem(i));
      });

      delete data.Items;
    }

    if(data.LastEvaluatedKey) {
      result.LastEvaluatedKey = data.LastEvaluatedKey;

      delete data.LastEvaluatedKey;
    }

    return callback(null, _.merge({}, data, result));
  };

};

Table.prototype.runQuery = function(params, callback) {
  var self = this;

  self.docClient.query(params, internals.deserializeItems(self, callback));
};

Table.prototype.runScan = function(params, callback) {
  var self = this;

  self.docClient.scan(params, internals.deserializeItems(self, callback));
};

Table.prototype.runBatchGetItems = function (params, callback) {

  var self = this;
  self.docClient.batchGetItem(params, callback);
};

internals.attributeDefinition = function (schema, key) {
  var type = schema._modelDatatypes[key];

  if(type === 'DATE') {
    type = 'S';
  }

  return {
    AttributeName : key,
    AttributeType : type
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

internals.secondaryIndex = function (schema, params) {
  var projection = params.projection || { ProjectionType : 'ALL' };

  return {
    IndexName : params.name,
    KeySchema : internals.keySchema(schema.hashKey, params.rangeKey),
    Projection : projection
  };
};

internals.globalIndex = function (indexName, params) {
  var projection = params.projection || { ProjectionType : 'ALL' };

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

  _.forEach(self.schema.secondaryIndexes, function (params) {
    attributeDefinitions.push(internals.attributeDefinition(self.schema, params.rangeKey));
    localSecondaryIndexes.push(internals.secondaryIndex(self.schema, params));
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

  self.docClient.createTable(params, callback);
};

Table.prototype.describeTable = function (callback) {

  var params = {
    TableName : this.tableName(),
  };

  this.docClient.describeTable(params, callback);
};

Table.prototype.deleteTable = function (callback) {
  callback = callback || _.noop;

  var params = {
    TableName : this.tableName(),
  };

  this.docClient.deleteTable(params, callback);
};

Table.prototype.updateTable = function (throughput, callback) {
  callback = callback || _.noop;

  var params = {
    TableName : this.tableName(),
    ProvisionedThroughput : {
      ReadCapacityUnits : throughput.readCapacity,
      WriteCapacityUnits : throughput.writeCapacity
    }
  };

  this.docClient.updateTable(params, callback);
};
