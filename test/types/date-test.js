'use strict';

var dateType = require('../../lib/types/date'),
    chai = require('chai');

chai.should();

describe('Date Type', function () {

  it('should return true for date object', function () {
    var d = dateType().required();

    d.validate(new Date()).should.be.true;
  });

  it('should return true for valid date format string', function () {
    var d = dateType().required();

    d.validate('2013-05-17T16:17:59.453Z').should.be.true;
  });

  it('should return false for invalid date format', function () {
    var d = dateType().required();

    d.validate('fail').should.be.false;
  });

  it('should return true for Date.now', function () {
    var d = dateType().required();

    d.validate(Date.now()).should.be.true;
  });

});
