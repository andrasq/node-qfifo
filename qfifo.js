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
var path = require('path');
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
    this.flushCbs = new Array();
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
QFifo.prototype.flush = function flush( callback ) {
    if (this.error) callback(this.error);
    else if (this.writeDoneCount >= this.writePendingCount) callback();
    else this.flushCbs.push({ awaitCount: this.writePendingCount, cb: callback });
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
        if (this._eof && this.readstringoffset >= this.readstring.length) { this.eof = true }
        return line;
    } else {
        // TODO: reading multi-chunk lines is inefficient, even the indexOf() is O(n^2)
        if (this.readstringoffset > 0) this.readstring = this.readstring.slice(this.readstringoffset);
        this.readstringoffset = 0;
        this._readsome();
        if (this._eof) { this.eof = true }
        return '';
    }
}

QFifo.prototype.pause = function pause( ) { this.readlinesPaused = true }
QFifo.prototype.resume = function resume( ) { this.readlinesPaused = false; this.readlinesLoop() }
QFifo.prototype.readlines = function readlines( visitor ) {
    var self = this, line;
    (function loop() {
        self.readlinesLoop = loop;
        // read-ahead next chunk while delivering lines from this one
        if (!self.readlinesPaused) self._readsome();
        while (!self.readlinesPaused && (line = self.getline())) visitor(line);
        // if no more lines but not eof yet _readsome will restart loop,
        // and if eof reached arrange to restart loop if file is appended
        if (self.eof && !self.readlinesPaused) {
            var watcher = _tryWatch(self.filename, function() {
                watcher.close();
                self._readsome();
            })
        }
    })();
}
function _tryWatch(file, cb) { try { return fs.watch(file, cb) } catch (err) { } }

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
            while (self.flushCbs.length && (self.flushCbs[0].awaitCount <= self.writeDoneCount || self.error)) {
                self.flushCbs.shift().cb(self.error);
            }
            if (self.writeDoneCount === self.writePendingCount) self.writeDoneCount = self.writePendingCount = 0;
            if (self.writestring) writechunk(); // keep writing if more data arrived
            else self.writing = false;
        })
    }
}

QFifo.prototype.batchCalls = function batchCalls( processBatchFunc, options ) {
    options = options || {};
    var maxWaitMs = options.maxWaitMs >= 0 ? options.maxWaitMs : 0;
    var maxBatchSize = options.maxBatchSize >= 1 ? options.maxBatchSize : 10;
    var startBatch = typeof options.startBatch === 'function' ? options.startBatch : null;
    var growBatch = typeof options.growBatch === 'function' ? options.growBatch : null;

    // prepare for the next (first) batch by processing the current (-1) batch
    var timer = null, itemCount = -1, batch = [], batchCbs = [];
    doProcessBatch();

    return function processItem(item, cb) {
        // add the item to the current batch
        growBatch ? growBatch(batch, item) : batch.push(item);
        cb && batchCbs.push(cb);
        itemCount += 1;
        // process the batch when the batch size and/or wait time are reached
        // if the allowed wait time is 0 then process the batch on next process tick
        if (itemCount >= maxBatchSize) doProcessBatch();
        else if (!timer) timer = (maxWaitMs > 0) ? setTimeout(doProcessBatch, maxWaitMs) : (process.nextTick(doProcessBatch), true);
    }

    // grab the items in the batch so far, and process them after an optional delay
    function doProcessBatch() {
        clearTimeout(timer);
        timer = null;
        var thisBatch = batch;
        var thisCbs = batchCbs;
        var thisItemCount = itemCount;
        // reuse the old batch if still empty, else start a new batch
        if (itemCount !== 0) {
            batch = startBatch ? startBatch(maxBatchSize) : new Array();
            batchCbs = new Array();
            itemCount = 0;
        }
        // batch processing is allowed to overlap; io inherently serializes, or use eg quickq
        // thisItemCount will only be 0 if a scheduled batch filled up and has already been processed
        // process the batch nextTick so if more items are added they will be processed in order
        if (thisItemCount > 0) process.nextTick(function() {
            processBatchFunc(thisBatch, function(err) {
                // an error received from the batch errors out all items in the batch
                for (var i = 0; i < thisCbs.length; i++) thisCbs[i](err);
                // TODO: should batch errors affect subsequent batches too?
            })
        })
    }
}

/*
 * rename name -> name.1, name.1 -> name.2, name.2 -> name.3 etc
 */
QFifo.prototype.rotateFiles = function rotateFiles( filename, callback ) {
    // TODO: escape regex metacharacters in filename
    var rotatedNames = new RegExp('^' + filename + '(\.([0-9]+))?$');
    this.matchFiles(path.basename(filename), rotatedNames, function(err, matches) {
        if (err) return callback(err, [err], []);
        var names = [], errors = [];
        matches = matches
            .sort(function(m1, m2) { return m1[2] && m2[2] ? m2[2] - m1[2] : m1[2] ? -1 : +1 });   // descending
        for (var i = 0; i < matches.length; i++) {
            var nextName = filename + '.' + (parseInt(matches[i][2] || '0') + 1);
            try { fs.renameSync(matches[i][0], nextName); names.push(nextName) }
            catch (err) { errors.push(err) };
            // TODO: retain the original timestamp on rotated files
        }
        callback(errors[0], errors, names);
    })
}

/*
 * return the matches for the regex among the filenames in the directory
 * For each match in matches[], match[0] is the original filename.
 */
QFifo.prototype.matchFiles = function matchFiles( dirname, matchRegex, callback ) {
    fs.readdir(dirname, function(err, files) {
        if (err) return callback(err, [err], []);
        var names = [], errors = [];
        var matches = files
            .map(function(name) { return name.match(matchRegex) })
            .filter(function(m1) { return !!m1 });
        callback(null, matches);
    })
}

// 43 usec fs.writeFile 200B async, 7.3 usec all sync, 4.3 usec if sync opened 'r+'
function writeHeaderSync( filename, contents ) {
    try { var fd = fs.openSync(filename, 'r+') } catch (e) { var fd = fs.openSync(filename, 'w') }
    var buf = allocBuf(200);
    var n = buf.write(contents);
    for ( ; n < buf.length; n++) buf[n] = 0x20;
    return fs.writeSync(fd, buf, 0, buf.length, null);
}

// aliases
QFifo.prototype.fflush = QFifo.prototype.flush;
QFifo.prototype.wsync = QFifo.prototype.flush;

QFifo.prototype = toStruct(QFifo.prototype);
function toStruct(hash) { return toStruct.prototype = hash }
