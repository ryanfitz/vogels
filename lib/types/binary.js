'use strict';

// Load modules

var Any = require('joi/lib/any');
var Errors = require('joi/lib/errors');
var Utils = require('joi/lib/utils');


// Declare internals

var internals = {};

module.exports = internals.Binary = function () {

    Any.call(this);
    this._type = 'binary';
    this._invalids.add('');
    this._invalids.add(new Buffer(''));

    this._base(function (value, state, options) {

        if (typeof value === 'string' ||
            value instanceof Buffer) {
            return null;
        }

        state.key = 'the value of ' + state.key + ' must be a either a string or a Buffer';
        return Errors.create('binary.base', null, state, options);
    });
};

Utils.inherits(internals.Binary, Any);


internals.Binary.create = function () {
    return new internals.Binary();
};
