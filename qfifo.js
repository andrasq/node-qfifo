/*
 * qfifo -- yet another stab
 *
 * Half-fifos: can be written or read, but not both at the same time.
 *
 * 2020-12-20 - AR.
 */

'use strict'

var fs = require('fs');
var sd = require('string_decoder');

module.exports = QFifo;

var allocBuf = eval('parseInt(process.versions.node) >= 6 ? Buffer.allocUnsafe : Buffer');
var fromBuf = eval('parseInt(process.versions.node) >= 6 ? Buffer.from : Buffer');

function QFifo( file, mode ) {
    this.file = file;
    this.mode = mode;
    this.head = { skip: 0 };
    this.eof = false;

    this.decoder = new sd.StringDecoder();
    this.readbuf = allocBuf(256 * 1024);
    this.readstring = '';
    this.readoffset = 0;
    this.writestring = '';
    this.writingCount = 0;
    this.writtenCount = 0;
    this.fd = -1;
    this.reading = false;
    this.writing = false;
    this.writeDelay = 2;
    this.writeCbs = new Array();
}

QFifo.prototype.open = function open( mode, cb ) {
    var self = this;
    if (this.fd >= 0) return cb(null, this.fd);
// FIXME: reopen where left off reading last time, so can stop part-way and resume
    fs.open(this.file, mode === 'r' ? 'r' : 'a', function(err, fd) {
        self.fd = err ? -1 : fd;
        cb(err, self.fd);
    });
}

QFifo.prototype.close = function close( ) {
    if (this.fd >= 0) fs.closeSync(this.fd);
    this.fd = -1;
}

QFifo.prototype.putline = function putline( str ) {
    // faster to append a newline to the string than to write a newline separately,
    // even though both are just concatenated to this.writestring.
    if (str[str.length - 1] !== '\n') str += '\n';
    this.write(str);
}

QFifo.prototype.fflush = function fflush( cb ) {
    if (this.writingCount > this.writtenCount) this.writeCbs.push({ count: this.writingCount, cb: cb });
    else cb();
}

// FIXME: need rsync to persist read state for to reopen

QFifo.prototype.getline = function getline( ) {
    // readoffset idea from qfgets
    var self = this, ix = this.readstring.indexOf('\n', this.readoffset);
    if (ix < 0) {
        if (this.readoffset > 0) this.readstring = this.readstring.slice(this.readoffset);
        this.readoffset = 0;
        this._getmore();
        return '';
    } else {
        var line = this.readstring.slice(this.readoffset, ix + 1);
        this.readoffset = ix + 1;
        return line;
    }
}

QFifo.prototype.write = function write( str ) {
    // faster to concat strings and write less (1.4mb in 14 vs 56 char chunks: .35 vs .11 sec)
    this.writingCount += str.length;
    this.writestring += str;
    this._writesome();
}

QFifo.prototype._getmore = function _getmore( ) {
    var self = this;
    if (!this.reading) {
        this.reading = true;
        fs.read(this.fd, this.readbuf, 0, this.readbuf.length, null, function(err, nbytes) {
            if (nbytes === 0) self.eof = true;
            self.reading = false;
            if (err) return cb(err); // FIXME: and clobber the input stream?
            if (nbytes > 0) {
                self.readstring += self.decoder.write(self.readbuf.slice(0, nbytes));
            }
        })
    }
}

QFifo.prototype._writesome = function _writesome( ) {
    if (!this.writing) {
        var self = this;
        self.writing = true;
        setTimeout(function writeit() {
            var nchars = self.writestring.length;
            var buf = fromBuf(self.writestring);
            self.writestring = '';
            fs.write(self.fd, buf, 0, buf.length, null, function(err, nbytes) {
// FIXME: handle write errors
                self.writtenCount += nchars;
                while (self.writeCbs.length && self.writeCbs[0].count <= self.writtenCount) {
                    self.writeCbs.shift().cb(err);
                }
                if (self.writestring) return writeit(); // keep writing if there is more
                self.writing = false;
            })
        }, self.writeDelay);
    }
}

// node-v0.11 and up take write(fd, data, callback) but older node need (fd, buff, offset, nbytes, filepos, callback)
// also, write() started accepting strings only with node-v0.11.5
// var writeFd = eval('true && utils.versionCompar(process.versions.node, "0.11.5") >= 0 '+
//     '? function writeFd(fd, data, cb) { fs.write(fd, data, cb) } ' +
//     ': function writeFd(fd, data, cb) { var buf = utils.fromBuf(data); fs.write(fd, buf, 0, buf.length, null, cb) }');
