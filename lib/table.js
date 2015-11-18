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

var Table = module.exports = function (name, schema, serializer, docClient, logger) {
  this.config = {name : name};
  this.schema = schema;
  this.serializer = serializer;
  this.docClient = docClient;
  this.log = logger;

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

Table.prototype.sendRequest = function (method, params, callback) {
  var self = this;

  var driver;
  if (_.isFunction(self.docClient[method])) {
    driver = self.docClient;
  } else if (_.isFunction(self.docClient.service[method])) {
    driver = self.docClient.service;
  }

  var startTime = Date.now();

  self.log.info({params : params}, 'vogels %s request', method.toUpperCase());
  driver[method].call(driver, params, function (err, data) {
    var elapsed = Date.now() - startTime;

    if (err) {
      self.log.warn({err : err}, 'vogels %s error', method.toUpperCase());
      return callback(err);
    } else {
      self.log.info({data : data}, 'vogels %s response - %sms', method.toUpperCase(), elapsed);
      return callback(null, data);
    }
  });
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

  self.sendRequest('get', params, function (err, data) {
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

  if (_.isArray(item)) {
    async.map(item, function (data, callback) {
      return internals.createItem(self, data, options, callback);
    }, callback);
  } else {
    return internals.createItem(self, item, options, callback);
  }
};

internals.createItem = function (table, item, options, callback) {
  var self = table;

  var start = function (callback) {
    var data = self.schema.applyDefaults(item);

    var paramName = _.isString(self.schema.createdAt) ? self.schema.createdAt : 'createdAt';

    if(self.schema.timestamps && self.schema.createdAt !== false && !_.has(data, paramName)) {
      data[paramName] = new Date().toISOString();
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
      internals.addConditionExpression(params, options.expected);
      options = _.omit(options, 'expected');
    }

    if (options.overwrite === false) {
      var expected = _.chain([self.schema.hashKey, self.schema.rangeKey]).compact().reduce(function (result, key) {
        _.set(result, key + '.<>', _.get(params.Item, key));
        return result;
      }, {}).value();

      internals.addConditionExpression(params, expected);
    }

    options = _.omit(options, 'overwrite'); // remove overwrite flag regardless if true or false

    params = _.merge({}, params, options);

    self.sendRequest('put', params, function (err) {
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
      item[paramName] = new Date().toISOString();
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
      internals.addConditionExpression(params, options.expected);
      delete options.expected;
    }

    params = _.chain({}).merge(params, options).omit(_.isEmpty).value();

    self.sendRequest('update', params, function (err, data) {
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

internals.addConditionExpression = function (params, expectedConditions) {
  _.each(expectedConditions, function (val, key) {
    var operator;
    var expectedValue = null;

    var existingValueKeys = _.keys(params.ExpressionAttributeValues);

    if (_.isObject(val) && _.isBoolean(val.Exists) && val.Exists === true) {
      operator = 'attribute_exists';
    } else if (_.isObject(val) && _.isBoolean(val.Exists) && val.Exists === false) {
      operator = 'attribute_not_exists';
    } else if (_.isObject(val) && _.has(val, '<>')) {
      operator = '<>';
      expectedValue = _.get(val, '<>');
    } else {
      operator = '=';
      expectedValue = val;
    }

    var condition = expressions.buildFilterExpression(key, operator, existingValueKeys, expectedValue, null);
    params.ExpressionAttributeNames  = _.merge({}, condition.attributeNames, params.ExpressionAttributeNames);
    params.ExpressionAttributeValues = _.merge({}, condition.attributeValues, params.ExpressionAttributeValues);

    if (_.isString(params.ConditionExpression )) {
      params.ConditionExpression = params.ConditionExpression + ' AND (' + condition.statement + ')';
    } else {
      params.ConditionExpression = '(' + condition.statement + ')';
    }
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
    internals.addConditionExpression(params, options.expected);

    delete options.expected;
  }

  params = _.merge({}, params, options);

  self.sendRequest('delete', params, function (err, data) {
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

  self.sendRequest('query', params, internals.deserializeItems(self, callback));
};

Table.prototype.runScan = function(params, callback) {
  var self = this;

  self.sendRequest('scan', params, internals.deserializeItems(self, callback));
};

Table.prototype.runBatchGetItems = function (params, callback) {

  var self = this;
  self.sendRequest('batchGet', params, callback);
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

  self.sendRequest('createTable', params, callback);
};

Table.prototype.describeTable = function (callback) {

  var params = {
    TableName : this.tableName(),
  };

  this.sendRequest('describeTable', params, callback);
};

Table.prototype.deleteTable = function (callback) {
  callback = callback || _.noop;

  var params = {
    TableName : this.tableName(),
  };

  this.sendRequest('deleteTable', params, callback);
};

Table.prototype.updateTable = function (throughput, callback) {
  var self = this;
  if (typeof throughput === 'function' && !callback) {
    callback = throughput;
    throughput = {};
  }

  callback = callback || _.noop;
  throughput = throughput || {};

  async.parallel([
    async.apply(internals.syncIndexes, self),
    async.apply(internals.updateTableCapacity, self, throughput),
  ], callback);
};

internals.updateTableCapacity = function (table, throughput, callback) {
  var params = {};

  if (_.has(throughput, 'readCapacity') || _.has(throughput, 'writeCapacity')) {
    params.ProvisionedThroughput = {};

    if (_.has(throughput, 'readCapacity')) {
      params.ProvisionedThroughput.ReadCapacityUnits = throughput.readCapacity;
    }

    if (_.has(throughput, 'writeCapacity')) {
      params.ProvisionedThroughput.WriteCapacityUnits = throughput.writeCapacity;
    }
  }

  if (!_.isEmpty(params)) {
    params.TableName = table.tableName();
    table.sendRequest('updateTable', params, callback);
  } else {
    return callback();
  }
};

internals.syncIndexes = function (table, callback) {
  callback = callback || _.noop;

  table.describeTable(function (err, data) {
    if (err) {
      return callback(err);
    }

    var missing = _.values(internals.findMissingGlobalIndexes(table, data));
    if (_.isEmpty(missing)) {
      return callback();
    }

    // UpdateTable only allows one new index per UpdateTable call
    // http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.OnlineOps.html#GSI.OnlineOps.Creating
    var maxIndexCreationsAtaTime = 5;
    async.mapLimit(missing, maxIndexCreationsAtaTime, function (params, callback) {
      var attributeDefinitions = [];

      if(!_.find(attributeDefinitions, { 'AttributeName': params.hashKey })) {
        attributeDefinitions.push(internals.attributeDefinition(table.schema, params.hashKey));
      }

      if(params.rangeKey && !_.find(attributeDefinitions, { 'AttributeName': params.rangeKey })) {
        attributeDefinitions.push(internals.attributeDefinition(table.schema, params.rangeKey));
      }

      var currentWriteThroughput = data.Table.ProvisionedThroughput.WriteCapacityUnits;
      var newIndexWriteThroughput = _.ceil(currentWriteThroughput * 1.5);
      params.writeCapacity = params.writeCapacity || newIndexWriteThroughput;

      table.log.info('adding index %s to table %s', params.name, table.tableName());

      var updateParams = {
        TableName : table.tableName(),
        AttributeDefinitions : attributeDefinitions,
        GlobalSecondaryIndexUpdates : [{Create : internals.globalIndex(params.name, params)}]
      };

      table.sendRequest('updateTable', updateParams, callback);
    }, callback);
  });
};

internals.findMissingGlobalIndexes = function (table, data) {
  if(_.isNull(data) || _.isUndefined(data)) {
    // table does not exist 
    return table.schema.globalIndexes;
  } else {
    var indexData = _.get(data, 'Table.GlobalSecondaryIndexes');
    var existingIndexNames = _.pluck(indexData, 'IndexName');

    var missing = _.reduce(table.schema.globalIndexes, function (result, idx, indexName) {
    if (!_.includes(existingIndexNames, idx.name)) {
        result[indexName] = idx;
    }

    return result;
    }, {});

    return missing;
  }
};
