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

var setImmediate = eval('global.setImmediate || process.nextTick');
var allocBuf = eval('parseInt(process.versions.node) >= 6 ? Buffer.allocUnsafe : Buffer');
var fromBuf = eval('parseInt(process.versions.node) >= 6 ? Buffer.from : Buffer');
var CH_NL = '\n'.charCodeAt(0);

function QFifo( filename, options ) {
    if (typeof options !== 'object') options = { flag: options };
    this.options = {
        flag:           getOption(options, 'flag', 'string', 'r'),
        readSize:       getOption(options, 'readSize', 'number', 64 * 1024),
        writeSize:      getOption(options, 'writeSize', 'number', 16 * 1024),
        writeDelay:     getOption(options, 'writeDelay', 'number', 2),
        updatePosition: getOption(options, 'updatePosition', 'boolean', true),
    };
    if (!filename) throw new Error('missing filename');
    var flag = this.options.flag;
    if (flag[0] !== 'r' && flag[0] !== 'a') throw new Error(flag + ": bad open mode, expected 'r' or 'a'");

    this.filename = filename;
    this.headername = filename + '.hd';
    this.fd = -1;

    this.eof = false;           // no more lines until more written
    this.error = null;          // read error, bad file
    this.position = 0;          // byte offset of next line to be read

    // TODO: move this into Reader()
    this._eof = false;          // short read, file eof
    this.reading = false;       // already reading mutex
    this.readbuf = allocBuf(this.options.readSize);
    this.decoder = new sd.StringDecoder();
    this.seekoffset = this.position;
    this.readstring = '';
    this.readstringoffset = 0;
    this.readlinesPaused = false;
    this.readlinesLoop = function(){};

    // TODO: move this into Writer()
    this.writing = false;       // already writing mutex
    this.writestring = '';
    this.writePendingCount = 0;
    this.writeDoneCount = 0;
    this.fflushCbs = new Array();
    this.openCbs = new Array();
    this.queuedWrite = null;
}
function getOption(opts, name, type, _default) {
    return typeof opts[name] === type ? opts[name] : opts[name] === undefined ? _default : throwError();
    function throwError() { throw new Error(name + ': must be ' + type) }
}

QFifo.prototype.open = function open( callback ) {
    var self = this;
    if (this.fd >= 0 || this.error) return callback(this.error, this.fd);
    this.openCbs.push(callback);
    if (this.fd === -1) this.fd = -2; else return; // mutex the openers
    fs.open(this.filename, this.options.flag, function(err, fd) {
        self.fd = err ? -1 : fd;
        if (err) { self.error = err; self.eof = self._eof = true; self._runCallbacks(self.openCbs, err, self.fd); return }
        fs.readFile(self.headername, function(err2, header) {
            try { var header = !err2 && JSON.parse(String(header)) || {} } catch (e) { var header = { position: 0 } }
            self.position = self.seekoffset = header.position >= 0 ? Number(header.position) : 0;
            self.eof = self._eof = false;
            self.error = null;
            self._runCallbacks(self.openCbs, err, self.fd); // call all the open callbacks
        })
    })
}
QFifo.prototype._runCallbacks = function _runCallbacks( cbs, err, ret ) { while (cbs.length) cbs.shift()(err, ret); }

QFifo.prototype.close = function close( ) {
    if (this.fd >= 0) try { fs.closeSync(this.fd) } catch (e) { console.error(e) }
    this.fd = -1;
}

QFifo.prototype.putline = function putline( str ) {
    // faster to append a newline here than to write a newline separately,
    // even though both are just concatenated to this.writestring.
    // TODO: allow for Buffers without converting first (to stream incoming data to fifo)
    if (typeof str !== 'string') str = Buffer.isBuffer(str) ? str.toString(): String(str);
    str.charCodeAt(str.length - 1) === CH_NL ? this.write(str) : this.write(str + '\n');
}

QFifo.prototype.write = function write( str ) {
    // faster to concat strings and write less often
    this.writePendingCount += str.length;
    this.writestring += str;
    var self = this;
    if (this.writestring.length >= this.options.writeSize) this._writesome();
    else if (!this.queuedWrite) this.queuedWrite =
        setTimeout(function() { self.queuedWrite = null; self._writesome() }, this.options.writeDelay);
}

// push the written data to the file
QFifo.prototype.fflush = function fflush( callback ) {
    if (this.error) callback(this.error);
    else if (this.writeDoneCount >= this.writePendingCount) callback();
    else this.fflushCbs.push({ awaitCount: this.writePendingCount, cb: callback });
}

// checkpoint the read header
QFifo.prototype.rsync = function rsync( callback ) {
    var header = { position: this.position };
    try { writeHeaderSync(this.headername, JSON.stringify(header)); callback() } catch (e) { callback(e) }
}

// tracking readstringoffset idea borrowed from qfgets
QFifo.prototype.getline = function getline( ) {
    var eol = this.readstring.indexOf('\n', this.readstringoffset);
    if (eol >= 0) {
        var line = this.readstring.slice(this.readstringoffset, this.readstringoffset = eol + 1);
        if (this.options.updatePosition) this.position += Buffer.byteLength(line);
        if (this.readstringoffset >= this.readstring.length) this.eof = this._eof;
        return line;
    } else {
        // TODO: reading multi-chunk lines is inefficient, even the indexOf() is O(n^2)
        if (this.readstringoffset > 0) this.readstring = this.readstring.slice(this.readstringoffset);
        this.readstringoffset = 0;
        this._readsome();
        this.eof = this._eof;
        return '';
    }
}

QFifo.prototype.pause = function pause( ) { this.readlinesPaused = true }
QFifo.prototype.resume = function resume( ) { this.readlinesPaused = false; this.readlinesLoop() }
QFifo.prototype.readlines = function readlines( visitor ) {
    var self = this, line;
    (function loop() {
        while (!self.readlinesPaused && (line = self.getline())) visitor(line.slice(0, -1));
        if (!self.eof || self.readlinesPaused) { self.readlinesLoop = loop; setImmediate(function() { self._readsome() }) }
    })();
}

QFifo.prototype._readsome = function _readsome( ) {
    var self = this;
    this.reading ? 'already reading, only one at a time' : (this.reading = true, this.open(readchunk));
    function readchunk() {
        if (self.error) { self.reading = false; return }
        fs.read(self.fd, self.readbuf, 0, self.readbuf.length, self.seekoffset, function(err, nbytes) {
            self.reading = false;
            self.seekoffset += nbytes;
            if (err) { self.error = err; self.eof = self._eof = true; return }
            self._eof = (!(nbytes === self.readbuf.length));
            var wasEmpty = !self.readstring;
            self.readstring += self.decoder.write(self.readbuf.slice(0, nbytes));
            // if first read into empty buffer, read ahead next chunk
            if (wasEmpty && nbytes) { self.reading = true; readchunk() }
            // if readlines is active, deliver the lines from this chunk
            // FIXME: if looping before readahead, next-to-last read will get nbytes=1000 from offset 25000 instead of 720
            // thus duplicated lines and no eof.  The read after that gets the 720 and stops.
            // Does not depend on node version, node-v0.10.42 behaves the same way, setImmediate(loop) same.
            if (nbytes > 0) self.readlinesLoop();
        })
    }
}

QFifo.prototype._writesome = function _writesome( ) {
    var self = this;
    this.writing ? 'arlready writing, only one at a time' : (this.writing = true, this.open(writechunk));
    function writechunk() {
        if (self.error) { self.writing = false; return }
        var nchars = self.writestring.length, writebuf = fromBuf(self.writestring);
        self.writestring = '';
        // node since v0.11.5 also accepts write(fd, string, cb), but the old api is faster
        fs.write(self.fd, writebuf, 0, writebuf.length, null, function(err, nbytes) {
            if (err) self.error = err; // and continue, to error out the pending callbacks
            self.writeDoneCount += nchars;
            while (self.fflushCbs.length && (self.fflushCbs[0].awaitCount <= self.writeDoneCount || self.error)) {
                self.fflushCbs.shift().cb(self.error);
            }
            if (self.writeDoneCount === self.writePendingCount) self.writeDoneCount = self.writePendingCount = 0;
            if (self.writestring) writechunk(); // keep writing if more data arrived
            else self.writing = false;
        })
    }
}

QFifo.prototype = toStruct(QFifo.prototype);

// 43 usec fs.writeFile 200B async, 7.3 usec all sync, 4.3 usec if sync opened 'r+'
function writeHeaderSync( filename, contents ) {
    try { var fd = fs.openSync(filename, 'r+') } catch (e) { var fd = fs.openSync(filename, 'w') }
    var buf = allocBuf(200);
    var n = buf.write(contents);
    for ( ; n < buf.length; n++) buf[n] = 0x20;
    return fs.writeSync(fd, buf, 0, buf.length, null);
}

function toStruct(hash) { return toStruct.prototype = hash }
