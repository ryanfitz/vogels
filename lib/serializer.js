'use strict';

var _ = require('lodash');

var serializer = module.exports;

var internals = {};

var serialize = internals.serialize = {
  string : function (value) {
    return {S: value};
  },

  number: function (value) {
    return {N: value.toString()};
  },

  binary: function (value) {
    return {B: new Buffer(value).toString('base64')};
  },

  date : function (value) {
    if(_.isDate(value)) {
      return {S: value.toISOString()};
    } else {
      return {S: new Date(value).toISOString()};
    }
  },

  boolean : function (value) {
    if (value && value !== 'false') {
      return {N: '1'};
    } else {
      return {N: '0'};
    }
  },

  stringSet : function (value) {
    if(_.isArray(value) ) {
      return {SS: _.map(value, function (v) { return v.toString(); }) };
    } else {
      return {SS: [value.toString()]};
    }
  },

  numberSet : function (value) {
    if(_.isArray(value) ) {
      return {NS: _.map(value, function (v) { return Number(v).toString(); }) };
    } else {
      return {NS: [Number(value).toString()]};
    }
  },

  binarySet : function (value) {
    if(_.isArray(value) ) {
      return {BS: _.map(value, function (v) { return new Buffer(v).toString('base64'); }) };
    } else {
      return {BS: [new Buffer(value).toString('base64')]};
    }
  }

};

var deserializer = internals.deserializer = {
  number : function (value) {
    if(value.N) {
      return Number(value.N);
    } else if (value.S) {
      return Number(value.S);
    } else if (value.B) {
      return Number(new Buffer(value.B, 'base64'));
    } else {
      return null;
    }
  },

  boolean: function (value) {
    if(value.N) {
      return Boolean(Number(value.N));
    } else if (value.S) {
      return value.S === 'true';
    } else if (value.B) {
      return new Buffer(value.B, 'base64').toString() === 'true';
    } else {
      return false;
    }
  },

  string : function (value) {
    if(value.S) {
      return value.S;
    } else if (value.N) {
      return value.N;
    } else if (value.B) {
      return new Buffer(value.B, 'base64').toString();
    } else {
      return null;
    }
  },

  binary : function (value) {
    if(value.B) {
      return new Buffer(value.B, 'base64');
    } else if (value.N) {
      return new Buffer(value.N);
    } else if (value.S) {
      return new Buffer(value.S);
    } else {
      return null;
    }
  },

  date : function (value) {
    if (value.S) {
      return new Date(value.S);
    } else if (value.N) {
      return new Date(value.N);
    } else if (value.B) {
      return new Date(new Buffer(value.B, 'base64'));
    } else {
      return null;
    }
  },

  numberSet : function (value) {
    if(value.NS) {
      return _.map(value.NS, function (numString){ return Number(numString);});
    } else if (value.SS) {
      return _.map(value.SS, function (numString){ return Number(numString);});
    } else if(value.BS) {
      return _.map(value.BS, function (base64String){ return Number(new Buffer(base64String, 'base64'));});
    } else if (value.S) {
      return [Number(value.S)];
    } else if (value.N) {
      return [Number(value.N)];
    } else {
      return [];
    }
  },

  stringSet : function (value) {
    if(value.SS) {
      return value.SS;
    } else if (value.NS) {
      return value.NS;
    } else if(value.BS) {
      return _.map(value.BS, function (base64String){ return new Buffer(base64String, 'base64').toString();});
    } else if (value.S) {
      return [value.S];
    } else if (value.N) {
      return [value.N];
    } else {
      return [];
    }
  },

  binarySet : function (value) {
    if(value.BS) {
      return _.map(value.BS, function (base64String){ return new Buffer(base64String, 'base64');});
    } else if(value.NS) {
      return _.map(value.NS, function (number){ return new Buffer(number);});
    } else if (value.SS) {
      return _.map(value.SS, function (string){ return new Buffer(string);});
    } else if (value.S) {
      return [new Buffer(value.S)];
    } else if (value.N) {
      return [new Buffer(value.N)];
    } else {
      return [];
    }
  }
};

internals.deserializeAttribute = function (value, attr) {
  if(!value || !attr) {
    return null;
  }

  var type = attr.type._type;

  switch(type){
  case 'string':
  case 'uuid':
  case 'timeuuid':
    return deserializer.string(value);
  case 'number':
    return deserializer.number(value);
  case 'binary':
    return deserializer.binary(value);
  case 'date':
    return deserializer.date(value);
  case 'boolean':
    return deserializer.boolean(value);
  case 'numberSet':
    return deserializer.numberSet(value);
  case 'stringSet':
    return deserializer.stringSet(value);
  case 'binarySet':
    return deserializer.binarySet(value);
  default:
    throw new Error('Unsupported schema type - ' + type);
  }
};

internals.serializeAttribute = function (value, attr, options) {
  if(!attr || _.isNull(value)) {
    return null;
  }

  options = options || {};

  var type = attr.type._type;

  switch(type){
  case 'string':
  case 'uuid':
  case 'timeuuid':
    return serialize.string(value);
  case 'number':
    return serialize.number(value);
  case 'binary':
    return serialize.binary(value);
  case 'date':
    return serialize.date(value);
  case 'boolean':
    return serialize.boolean(value);
  case 'numberSet':
    if(options.convertSets) {
      return serialize.number(value);
    }
    return serialize.numberSet(value);
  case 'stringSet':
    if(options.convertSets) {
      return serialize.string(value);
    }
    return serialize.stringSet(value);
  case 'binarySet':
    if(options.convertSets) {
      return serialize.binary(value);
    }
    return serialize.binarySet(value);
  default:
    throw new Error('Unsupported schema type - ' + type);
  }
};

serializer.buildKey = function (hashKey, rangeKey, schema) {
  var obj = {};

  if(_.isPlainObject(hashKey)) {
    obj[schema.hashKey] = hashKey[schema.hashKey];

    if(schema.rangeKey) {
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

    _.each(schema.secondaryIndexes, function (rangeKey) {
      if(_.has(hashKey, rangeKey)){
        obj[rangeKey] = hashKey[rangeKey];
      }
    });

  } else {
    obj[schema.hashKey] = hashKey;

    if(schema.rangeKey) {
      obj[schema.rangeKey] = rangeKey;
    }
  }

  return serializer.serializeItem(schema, obj);
};

serializer.serializeItem = function (schema, item, options) {
  options = options || {};

  if(!item) {
    return null;
  }

  var serialized = _.reduce(schema.attrs, function (result, attr, key) {
    if(_.has(item, key)) {

      if(options.expected && _.isObject(item[key]) && _.isBoolean(item[key].Exists)) {
        result[key] = item[key];
        return result;
      }

      var val = internals.serializeAttribute(item[key], attr, options);

      if(!_.isNull(val) || options.returnNulls) {
        if(options.expected) {
          result[key] = {'Value' : val};
        } else {
          result[key] = val;
        }
      }
    }

    return result;
  }, {});

  return serialized;
};

serializer.serializeItemForUpdate = function (schema, action, item) {

  return _.reduce(schema.attrs, function (result, attr, key) {
    if(_.has(item, key) && key !== schema.hashKey && key !== schema.rangeKey) {
      var value = item[key];
      if(_.isNull(value)) {
        result[key] = {Action : 'DELETE'};
      } else if (_.isPlainObject(value) && value.$add) {
        result[key] = {Action : 'ADD', Value: internals.serializeAttribute(value.$add, attr)};
      } else if (_.isPlainObject(value) && value.$del) {
        result[key] = {Action : 'DELETE', Value: internals.serializeAttribute(value.$del, attr)};
      } else {
        result[key] = {Action : action, Value: internals.serializeAttribute(value, attr)};
      }
    }

    return result;
  }, {});

};

serializer.deserializeItem = function (schema, item) {

  if(!item) {
    return null;
  }

  var deserialized = _.reduce(schema.attrs, function (result, attr, key) {
    var value = internals.deserializeAttribute(item[key], attr);

    if(!_.isNull(value) && !_.isUndefined(value)) {
      result[key] = value;
    }

    return result;
  }, {});

  return deserialized;
};

serializer.deserializeKeys = function (schema, item) {
  var result = {};

  result[schema.hashKey] = internals.deserializeAttribute(item[schema.hashKey], schema.attrs[schema.hashKey]);

  if(schema.rangeKey) {
    result[schema.rangeKey] = internals.deserializeAttribute(item[schema.rangeKey], schema.attrs[schema.rangeKey]);
  }

  _.each(schema.globalIndexes, function (keys) {
    if(item[keys.hashKey]){
      result[keys.hashKey] = internals.deserializeAttribute(item[keys.hashKey], schema.attrs[keys.hashKey]);
    }

    if(item[keys.rangeKey]){
      result[keys.rangeKey] = internals.deserializeAttribute(item[keys.rangeKey], schema.attrs[keys.rangeKey]);
    }
  });

  _.each(schema.secondaryIndexes, function (rangekey) {
    if(item[rangekey]){
      result[rangekey] = internals.deserializeAttribute(item[rangekey], schema.attrs[rangekey]);
    }
  });

  return result;
};
