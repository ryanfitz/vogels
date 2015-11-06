'use strict';

var _     = require('lodash'),
    utils = require('./utils'),
    AWS   = require('aws-sdk');

var serializer = module.exports;

var internals = {};

internals.docClient = new AWS.DynamoDB.DocumentClient();

internals.createSet = function(value) {
  if(_.isArray(value) ) {
    return internals.docClient.createSet(value);
  } else {
    return internals.docClient.createSet([value]);
  }
};

var serialize = internals.serialize = {

  binary: function (value) {
    if(_.isString(value)) {
      return utils.strToBin(value);
    }

    return value;
  },

  date : function (value) {
    if(_.isDate(value)) {
      return value.toISOString();
    } else {
      return new Date(value).toISOString();
    }
  },

  boolean : function (value) {
    if (value && value !== 'false') {
      return true;
    } else {
      return false;
    }
  },

  stringSet : function (value) {
    return internals.createSet(value, 'S');
  },

  numberSet : function (value) {
    return internals.createSet(value, 'N');
  },

  binarySet : function (value) {
    var bins = value;
    if(!_.isArray(value)) {
      bins = [value];
    }

    var vals = _.map(bins, serialize.binary);
    return internals.createSet(vals, 'B');
  }
};

internals.deserializeAttribute = function (value) {
  if(_.isObject(value) && _.isFunction(value.detectType) && _.isArray(value.values)) {
    // value is a Set object from document client 
    return value.values;
  } else {
    return value;
  }
};

internals.serializeAttribute = serializer.serializeAttribute = function (value, type, options) {
  if(!type) { // if type is unknown, possibly because its an dynamic key return given value
    return value;
  }

  if(_.isNull(value)) {
    return null;
  }

  options = options || {};

  switch(type){
  case 'DATE':
    return serialize.date(value);
  case 'BOOL':
    return serialize.boolean(value);
  case 'B':
    return serialize.binary(value);
  case 'NS':
    return serialize.numberSet(value);
  case 'SS':
    return serialize.stringSet(value);
  case 'BS':
    return serialize.binarySet(value);
  default:
    return value;
  }
};

serializer.buildKey = function (hashKey, rangeKey, schema) {
  var obj = {};

  if(_.isPlainObject(hashKey)) {
    obj[schema.hashKey] = hashKey[schema.hashKey];

    if(schema.rangeKey && !_.isNull(hashKey[schema.rangeKey]) && !_.isUndefined(hashKey[schema.rangeKey])) {
      obj[schema.rangeKey] = hashKey[schema.rangeKey];
    }
    _.each(schema.globalIndexes, function (keys) {
      if(_.has(hashKey, keys.hashKey)){
        obj[keys.hashKey] = hashKey[keys.hashKey];
      }

      if(_.has(hashKey, keys.rangeKey)){
        obj[keys.rangeKey] = hashKey[keys.rangeKey];
      }
    });

    _.each(schema.secondaryIndexes, function (keys) {
      if(_.has(hashKey, keys.rangeKey)){
        obj[keys.rangeKey] = hashKey[keys.rangeKey];
      }
    });

  } else {
    obj[schema.hashKey] = hashKey;

    if(schema.rangeKey && !_.isNull(rangeKey) && !_.isUndefined(rangeKey)) {
      obj[schema.rangeKey] = rangeKey;
    }
  }

  return serializer.serializeItem(schema, obj);
};

serializer.serializeItem = function (schema, item, options) {
  options = options || {};

  var serialize = function (item, datatypes) {
    datatypes = datatypes || {};

    if(!item) {
      return null;
    }

    return _.reduce(item, function (result, val, key) {
      if(options.expected && _.isObject(val) && _.isBoolean(val.Exists)) {
        result[key] = val;
        return result;
      }

      if(_.isPlainObject(val)) {
        result[key] = serialize(val, datatypes[key]);
        return result;
      }

      var attr = internals.serializeAttribute(val, datatypes[key], options);

      if(!_.isNull(attr) || options.returnNulls) {
        if(options.expected) {
          result[key] = {'Value' : attr};
        } else {
          result[key] = attr;
        }
      }

      return result;
    }, {});
  };

  return serialize(item, schema._modelDatatypes);
};

serializer.serializeItemForUpdate = function (schema, action, item) {
  var datatypes = schema._modelDatatypes;

  var data = utils.omitPrimaryKeys(schema, item);
  return _.reduce(data, function (result, value, key) {
    if(_.isNull(value)) {
      result[key] = {Action : 'DELETE'};
    } else if (_.isPlainObject(value) && value.$add) {
      result[key] = {Action : 'ADD', Value: internals.serializeAttribute(value.$add, datatypes[key])};
    } else if (_.isPlainObject(value) && value.$del) {
      result[key] = {Action : 'DELETE', Value: internals.serializeAttribute(value.$del, datatypes[key])};
    } else {
      result[key] =  {Action : action, Value: internals.serializeAttribute(value, datatypes[key])};
    }

    return result;
  }, {});
};

serializer.deserializeItem = function (item) {

  if(_.isNull(item)) {
    return null;
  }

  var formatter = function (data) {
    var map = _.mapValues;

    if(_.isArray(data)) {
      map = _.map;
    }

    return map(data, function(value) {
      var result;

      if(_.isPlainObject(value)) {
        result = formatter(value);
      } else if(_.isArray(value)) {
        result = formatter(value);
      } else {
        result = internals.deserializeAttribute(value);
      }

      return result;
    });
  };

  return formatter(item);
};
