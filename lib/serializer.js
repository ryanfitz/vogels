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
  }

};

var deserializer = internals.deserializer = {
  number : function (value) {
    if(value.N) {
      return Number(value.N);
    } else if (value.S) {
      return Number(value.S);
    } else {
      return null;
    }
  },

  boolean: function (value) {
    if(value.N) {
      return Boolean(Number(value.N));
    } else if (value.S) {
      return value.S === 'true';
    } else {
      return false;
    }
  },

  string : function (value) {
    if(value.S) {
      return value.S;
    } else if (value.N) {
      return value.N;
    } else {
      return null;
    }
  },

  date : function (value) {
    if (value.S) {
      return new Date(value.S);
    } else if (value.N) {
      return new Date(value.N);
    } else {
      return null;
    }
  },

  numberSet : function (value) {
    if(value.NS) {
      return _.map(value.NS, function (numString){ return Number(numString);});
    } else if (value.SS) {
      return _.map(value.SS, function (numString){ return Number(numString);});
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
    } else if (value.S) {
      return [value.S];
    } else if (value.N) {
      return [value.N];
    } else {
      return [];
    }
  }
};

internals.deserializeAttribute = function (value, attr) {
  if(!value || !attr) {
    return null;
  }

  var type = attr.type.type;

  switch(type){
  case 'String':
    return deserializer.string(value);
  case 'Number':
    return deserializer.number(value);
  case 'Date':
    return deserializer.date(value);
  case 'Boolean':
    return deserializer.boolean(value);
  case 'NumberSet':
    return deserializer.numberSet(value);
  case 'StringSet':
    return deserializer.stringSet(value);
  default:
    throw new Error('Unsupported schema type - ' + type);
  }
};

internals.serializeAttribute = function (value, attr, options) {
  if(!attr) {
    return null;
  }

  options = options || {};

  var type = attr.type.type;

  switch(type){
  case 'String':
    return serialize.string(value);
  case 'Number':
    return serialize.number(value);
  case 'Date':
    return serialize.date(value);
  case 'Boolean':
    return serialize.boolean(value);
  case 'NumberSet':
    if(options.convertSets) {
      return serialize.number(value);
    }
    return serialize.numberSet(value);
  case 'StringSet':
    if(options.convertSets) {
      return serialize.string(value);
    }
    return serialize.stringSet(value);
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
  } else {
    obj[schema.hashKey] = hashKey;

    if(schema.rangeKey) {
      obj[schema.rangeKey] = rangeKey;
    }
  }

  return serializer.serializeItem(schema, obj);
};

serializer.serializeItem = function (schema, item, options) {
  if(!item) {
    return null;
  }

  var serialized = _.reduce(schema.attrs, function (result, attr, key) {
    if(_.has(item, key)) {
      result[key] = internals.serializeAttribute(item[key], attr, options);
    }

    return result;
  }, {});

  return serialized;
};

serializer.serializeItemForUpdate = function (schema, action, item) {
  var serialized = serializer.serializeItem(schema, item);

  return _.reduce(serialized, function (result, value, key) {
    if(key !== schema.hashKey) {
      result[key] = {Action : action, Value: value};
    }

    return result;
  }, {});
};

serializer.deserializeItem = function (schema, item) {

  if(!item) {
    return null;
  }

  var deserialized = _.reduce(schema.attrs, function (result, attr, key) {
    result[key] = internals.deserializeAttribute(item[key], attr);

    return result;
  }, {});

  return deserialized;
};
