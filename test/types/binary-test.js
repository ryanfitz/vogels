'use strict';

var binaryType = require('../../lib/types/binary').create,
    chai = require('chai'),
    expect = chai.expect,
    zlib = require('zlib');

chai.should();

describe('Binary Type', function () {

  it('should return null for buffer object', function (done) {
    var d = binaryType().required(),
        input = '.................................';

    zlib.deflate(input, function(err, buffer) {
      if (!err) {
        try {
          expect(d.validate(buffer)).to.be.null;
          done();
        } catch (e) {
          done(e);
          return;
        }
      } else {
        done(err);
        return;
      }
    });
    
  });

  it('should return null for string', function () {
    var d = binaryType().required();

    expect(d.validate('foo')).to.be.null;
  });

  it('should return error for invalid format', function () {
    var d = binaryType().required();

    expect(d.validate(NaN)).to.be.instanceof(Error);
  });

});
