/*
 * qfifo -- quick file-based fifo to buffer newline terminated data
 *
 * Copyright (C) 2020 Andras Radics
 * Licensed under the Apache License, Version 2.0
 *
 * 2020-12-20 - AR.
 */

'use strict'

var fs = require('fs');
var sd = require('string_decoder');

module.exports = QFifo;

var allocBuf = eval('parseInt(process.versions.node) >= 6 ? Buffer.allocUnsafe : Buffer');
var fromBuf = eval('parseInt(process.versions.node) >= 6 ? Buffer.from : Buffer');

function QFifo( filename, options ) {
    if (typeof options !== 'object') options = { flag: options };
    var optionTypes = { flag: 'string', readSize: 'number', writeSize: 'number', writeDelay: 'number' };
    for (var k in optionTypes) {
        var value = options[k];
        if (value !== undefined && typeof value !== optionTypes[k]) throw new Error(k + ': must be a ' + optionTypes[k]);
    }
    if (!filename) throw new Error('missing filename');
    var flag = options.flag || 'r';
    if (flag[0] !== 'r' && flag[0] !== 'a') throw new Error(flag + ": bad open mode, expected 'r' or 'a'");

    this.filename = filename;
    this.headername = filename + '.hd';
    this.flag = flag;
    this.fd = -1;

    this.eof = false;           // no data from last read
    this.error = null;          // read error, bad file
    this.position = 0;          // byte offset of next line to be read

    // TODO: move this into Reader()
    this.reading = false;
    this.readSize = options.readSize || 32 * 1024;
    this.readbuf = allocBuf(this.readSize);
    this.decoder = new sd.StringDecoder();
    this.seekposition = this.position;
    this.readstring = '';
    this.readstringoffset = 0;

    // TODO: move this into Writer()
    this.writing = false;
    this.writeSize = options.writeSize || 32 * 1024;
    this.writeDelay = options.writeDelay || 2;
    this.writestring = '';
    this.writingCount = 0;
    this.writtenCount = 0;
    this.writeCbs = new Array();
    this.openCbs = new Array();
}

QFifo.prototype.open = function open( callback ) {
    var self = this;
    if (this.fd >= 0 || this.error) return callback(this.error, this.fd);
    this.openCbs.push(callback);
    if (this.fd === -1) {
        this.fd = -2;
        fs.open(this.filename, this.flag, function(err, fd) {
            self.fd = err ? -1 : fd;
            if (err) { self.error = err; self.eof = true; _runCallbacks(self.openCbs, err, self.fd); return }
            fs.readFile(self.headername, function(err2, header) {
                try { var header = JSON.parse(String(header)) || {} } catch (e) { var header = { position: 0 } }
                self.position = self.seekposition = header.position || 0;
                self.eof = false;
                self.error = null;
                _runCallbacks(self.openCbs, err, self.fd);
            })
        });
    }
    function _runCallbacks(cbs, err, ret) { while (cbs.length) cbs.shift()(err, ret) }
}

QFifo.prototype.close = function close( ) {
    if (this.fd >= 0) try { fs.closeSync(this.fd) } catch (e) { console.error(e) }
    this.fd = -1;
}

QFifo.prototype.putline = function putline( str ) {
    // faster to append a newline to the string than to write a newline separately,
    // even though both are just concatenated to this.writestring.
    if (str[str.length - 1] !== '\n') str += '\n';
    this.write(str);
}

QFifo.prototype.write = function write( str ) {
    // faster to concat strings and write less (1.4mb in 14 vs 56 char chunks: .35 vs .11 sec)
    this.writingCount += str.length;
    this.writestring += str;
    // TODO: write once writeSize has been reached
    // TODO: apply writeDelay
    this._writesome();
}

// push the written data to the file
QFifo.prototype.fflush = function fflush( callback ) {
    if (this.error) callback(this.error);
    else if (this.writingCount <= this.writtenCount) callback();
    else this.writeCbs.push({ count: this.writingCount, cb: callback });
}

// checkpoint the read header
// Note: this version is kinda slow, do not call often.
QFifo.prototype.rsync = function rsync( callback ) {
    var header = { position: this.position };
    fs.writeFile(this.headername, JSON.stringify(header), callback);
}

// tracking readstringoffset idea borrowed from qfgets
QFifo.prototype.getline = function getline( ) {
    if (this.readstring.length < 4 * this.readSize) this._readsome();
    var ix = this.readstring.indexOf('\n', this.readstringoffset);
    if (ix < 0) {
        if (this.readstringoffset > 0) this.readstring = this.readstring.slice(this.readstringoffset);
        this.readstringoffset = 0;
        return '';
    } else {
        var line = this.readstring.slice(this.readstringoffset, ix + 1);
        this.readstringoffset = ix + 1;
        // TODO: maybe find eoln() newlines in the buffer, remember line offets
        this.position += Buffer.byteLength(line); // track the read offset
        return line;
    }
}

QFifo.prototype._readsome = function _readsome( ) {
    if (!this.reading) {
        var self = this;
        self.reading = true;
        this.open(function(err) {
            if (self.error) { self.reading = false; return }
            fs.read(self.fd, self.readbuf, 0, self.readbuf.length, self.seekposition, function(err, nbytes) {
                if (err) { self.error = err; self.eof = true; return }
                self.eof = (nbytes === 0);
                self.seekposition += nbytes;
                self.reading = false;
                if (nbytes > 0) {
                    self.readstring += self.decoder.write(self.readbuf.slice(0, nbytes));
                }
            })
        })
    }
}

QFifo.prototype._writesome = function _writesome( ) {
    if (!this.writing) {
        var self = this;
        self.writing = true;
        this.open(function(err) {
            if (self.error) { self.writing = false; return }
            writeit();
            function writeit() {
                var nchars = self.writestring.length;
                var buf = fromBuf(self.writestring); // one-shot write is faster than chunking
                self.writestring = '';
                // node since v0.11.5 also accepts write(fd, string, cb), but the old api is faster
                fs.write(self.fd, buf, 0, buf.length, null, function(err, nbytes) {
                    if (err) self.error = err; // and continue, to error out all the pending callbacks
                    self.writtenCount += nchars;
                    while (self.writeCbs.length && (self.writeCbs[0].count <= self.writtenCount || self.error)) {
                        self.writeCbs.shift().cb(self.error);
                    }
                    if (self.writestring) writeit(); // keep writing if more data arrived
                    else self.writing = false;
                })
            }
        })
    }
}

QFifo.prototype = toStruct(QFifo.prototype);

/**
function eoln( buf, pos ) {
    for (var i=pos; i<buf.length; i++) if (buf[i] === 10) return i;
    return -1;
}
**/

function toStruct(hash) { return toStruct.prototype = hash }
