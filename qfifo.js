/*
 * qfifo -- quick file-based fifo to buffer newline terminated data
 *
 * Copyright (C) 2020-2022 Andras Radics
 * Licensed under the Apache License, Version 2.0
 *
 * 2020-12-20 - AR.
 */

'use strict'

var fs = require('fs');
var path = require('path');
var sd = require('string_decoder');

module.exports = QFifo;

// setImmediate new in node-v0.9
var setImmediate = eval('global.setImmediate || process.nextTick');
// node-v0.8.6 renamed truncate to ftruncate(2) and added new truncate(2)
var ftruncateName = eval('fs.ftruncate ? "ftruncate" : "truncate"');
var allocBuf = eval('parseInt(process.versions.node) >= 6 ? Buffer.allocUnsafe : Buffer');
var fromBuf = eval('parseInt(process.versions.node) >= 6 ? Buffer.from : Buffer');
var CH_NL = '\n'.charCodeAt(0);
var CH_SP = ' '.charCodeAt(0);

function QFifo( filename, options ) {
    if (typeof options !== 'object') options = { flag: options };
    this.options = {
        flag:           getOption(options, 'flag', 'string', 'r'),
        readSize:       getOption(options, 'readSize', 'number', 64 * 1024),
        writeSize:      getOption(options, 'writeSize', 'number', 16 * 1024),
        writeDelay:     getOption(options, 'writeDelay', 'number', 2),
        reopenInterval: getOption(options, 'reopenInterval', 'number', 20),
        updatePosition: getOption(options, 'updatePosition', 'boolean', true),
        headerName:     getOption(options, 'headerName', 'string', ''),
    };
    if (!filename) throw new Error('missing filename');

    // all reading is done by explicit read offset, all writing by appending, hence only modes r, r+, a, a+
    // because of this, however, in-place header updates only possible in r+ mode
    var flag = this.options.flag;
    if (flag[0] !== 'r' && flag[0] !== 'a') throw new Error(flag + ": bad open mode, expected 'r' or 'a'");

    this.filename = filename;
    this.headername = this.options.headerName || filename + '.hd';
    this.fd = -1;
    this.reopenTime = -1;
    this.hasheader = this.filename === this.headername;

    var dataOffset = this.hasheader ? 200 : 0;

    this.eof = false;                   // no more lines until more written
    this.error = null;                  // read error, bad file
    this.dataOffset = dataOffset;       // offset in fifo of data lines after any embedded header
    this.position = this.dataOffset;    // byte offset of next line to be read

    // TODO: move this into Reader()
    this._eof = false;          // short read, file eof
    this.reading = false;       // already reading mutex
    this.readbuf = allocBuf(Math.max(this.options.readSize, this.dataOffset));
    this.decoder = new sd.StringDecoder();
    this.seekoffset = this.position;
    this.readstring = '';
    this.readstringoffset = 0;
    this.readlinesPaused = false;
    this.readlinesLoop = function(){};
    this.lastReadTime = 0;

    // TODO: move this into Writer()
    this.writing = false;       // already writing mutex
    this.compacting = false;    // repacking file
    this.writestring = '';
    this.writePendingCount = 0;
    this.writeDoneCount = 0;
    this.flushCbs = new Array();
    this.openCbs = new Array();
    this.queuedWrite = null;
}
function getOption(opts, name, type, _default) {
    return typeof opts[name] === type ? opts[name] : typeof opts[name] === 'undefined' ? _default : throwError();
    function throwError() { throw new Error(name + ': must be ' + type) }
}

/*
 * Open the fifo for access.
 * This call also implements reopenInterval, so the returned file descriptor is always fresh.
 */
QFifo.prototype.open = function open( callback ) {
    var self = this, reopenOnly;
    if (this.fd >= 0 && this.reopenTime > 0 && new Date() > this.reopenTime) { reopenOnly = true; this.close() }
    if (this.fd >= 0 || this.error) return callback(this.error, this.fd);
    this.openCbs.push(callback);
    if (this.fd === -1) this.fd = -2; else return; // mutex the openers
    fs.open(this.filename, this.options.flag, function(err, fd) {
        self.fd = err ? -1 : fd;
        if (err) return whenOpen(err);
        if (self.options.reopenInterval > 0) self.reopenTime = Date.now() + self.options.reopenInterval;
        if (reopenOnly) return whenOpen();
        var headerSize = self.hasheader ? self.dataOffset : 200;
        self.readHeaderFile(self.headername, self.readbuf, headerSize, function(err2, header) {
            if (err2 && err2.code !== 'ENOENT') return whenOpen(err2);
            try { header = !err2 && JSON.parse(header) || {} } catch (e) { var header = {} }
            self.dataOffset = header.skip >= 0 ? Number(header.skip) : self.dataOffset;
            self.position = self.seekoffset = header.position >= 0 ? Number(header.position) : self.dataOffset;
            self.lastReadTime = header.rtime || 0;
            self.eof = self._eof = false;
            self.error = null;
            // leave space for an in-file header if fifo is completely empty
            (self.dataOffset > 0 && self.options.flag[0] === 'a') ? self.skipDataOffset(whenOpen) : whenOpen();
        })
        function whenOpen(err3) {
            if (err3) { self.close(); self.error = err3; self.eof = self._eof = true }
            self._runCallbacks(self.openCbs, err3, self.fd); // notify all callers that the fifo is open
        }
    })
}
QFifo.prototype._runCallbacks = function _runCallbacks( cbs, err, ret ) { while (cbs.length) cbs.shift()(err, ret); }

QFifo.prototype.close = function close( callback ) {
    if (this.fd >= 0) try { fs.closeSync(this.fd) } catch (e) { var err = e; console.error(e) }
    this.fd = -1;
    if (callback) callback(err);
}

QFifo.prototype.putline = function putline( str ) {
    // faster to append a newline here than to write a newline separately,
    // even though both are just concatenated to this.writestring.
    // TODO: allow for Buffers without converting first (to stream incoming data to fifo)
    // note: buffers must contain entire lines
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
// 70% faster to open r+ when possible rather than w+
// 25% faster to build the json string by hand than to stringify an object
// note: rsync() is synchronous, so can use the already open fd (else would need to guard with this.open())
QFifo.prototype.rsync = function rsync( callback ) {
    // can update contained r+ mode fifo headers via fifo fd, but cannot seek on a+ mode append-only fifos
    if (this.hasheader && this.options.flag === 'r+' && this.fd >= 0) var fd = this.fd;
    else try { fd = fs.openSync(this.headername, 'r+') } catch (e) { fd = fs.openSync(this.headername, 'w+') }

    var header = '{"v":0,"skip":' + this.dataOffset + ',"position":' + this.position + ',"rtime":' + this.lastReadTime + '}';
    var headerSize = this.hasheader ? this.dataOffset : 200;
    try { this.writeHeaderSync(fd, header, this.readbuf, headerSize); callback() } catch (e) { callback(e) }
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

// deliver all lines in the fifo to `visitor(line)`
// Only one reader at a time, a second call to readline() will displace the first reader
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
        // if eof hit or file not yet exists, arrange to restart loop when file is appended
        var watcher = (!self.readlinesPaused) && _tryWatch(self.filename, function() {
            watcher.close();
            self._readsome();
        })
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
            self.lastReadTime = Date.now();
            // if first read into empty buffer, read ahead next chunk
            if (wasEmpty && nbytes) { self.reading = true; self.open(readchunk) }
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
        if (self.error) { self.writing = false; self._runFlushCallbacks(); return }
        var nchars = self.writestring.length, writebuf = fromBuf(self.writestring);
        self.writestring = '';
        // node since v0.11.5 also accepts write(fd, string, cb), but the old api is faster
        fs.write(self.fd, writebuf, 0, writebuf.length, null, function(err, nbytes) {
            if (err) self.error = err; // and continue, to error out the pending callbacks
            else if (nbytes > 0) self.eof = self._eof = false;
            self.writeDoneCount += nchars;
            if (self.flushCbs.length) self._runFlushCallbacks();
            if (self.writeDoneCount === self.writePendingCount) self.writeDoneCount = self.writePendingCount = 0;
            if (self.writestring) self.open(writechunk); // keep writing if more data arrived
            else self.writing = false;
        })
    }
}
QFifo.prototype._runFlushCallbacks = function _doFlush( ) {
    // TODO: would be faster to swap arrays than to shift, but code depends on this construct
    while (this.flushCbs.length && (this.flushCbs[0].awaitCount <= this.writeDoneCount || this.error)) {
        this.flushCbs.shift().cb(this.error);
    }
}

QFifo.prototype.batchCalls = function batchCalls( options, processBatchFunc ) {
    if (!processBatchFunc) { processBatchFunc = options; options = {} }
    var maxWaitMs = options.maxWaitMs >= 0 ? options.maxWaitMs : 0;
    var maxBatchSize = options.maxBatchSize >= 1 ? options.maxBatchSize : 10;
    var startBatch = typeof options.startBatch === 'function' ? options.startBatch : null;
    var growBatch = typeof options.growBatch === 'function' ? options.growBatch : null;

    // prepare for the next (first) batch by processing the current (-1) batch
    var timer = null, itemCount = -1, batch = [], batchCbs = [];
    doProcessBatch();

    return function processItem(item, cb) {
        // add the item to the current batch
        growBatch ? (batch = growBatch(batch, item)) : batch.push(item);
        cb && batchCbs.push(cb);
        itemCount += 1;
        // process the batch when the batch size and/or wait time are reached
        // if the allowed wait time is 0 then process the batch on next process tick
        if (itemCount >= maxBatchSize) doProcessBatch();
        else if (!timer) timer = (maxWaitMs > 0) ? setTimeout(doProcessBatch, maxWaitMs) : (process.nextTick(doProcessBatch), true);
    }

    // grab the items in the batch so far, and process them
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
 * in-place compact the fifo to free up unused space
 */
QFifo.prototype.compact = function compact( options, callback ) {
    if (typeof options === 'function') { callback = options; options = {} }
    var minSize = options.minSize >= 0 ? options.minSize : 1e6;
    var minReadRatio = options.minReadRatio >= 0 ? options.minReadRatio : 0.667; // NB!: ratio < 0.5 copy overlaps data
    var readSize = options.readSize > 0 ? options.readSize : this.options.readSize;
    var dstOffset = options.dstPosition || this.dataOffset;
    var self = this;
    self.open(function(err) {
        if (err) return callback(err);
        fs.stat(self.filename, function(err, stats) {
            if (err || stats.size < minSize || self.position / stats.size < minReadRatio) return callback(err);
            // TODO: mutex reading/writing against compacting too
            self.compacting = true;
            var buf = allocBuf(readSize);
            self.copyBytes(self.fd, self.fd, self.position, Infinity, dstOffset, buf, function(err, endPosition) {
                if (err) { self.compacting = false; return callback(err) }
                fs[ftruncateName](self.fd, endPosition, function(err) {
                    // TODO: make compact() reusable, move this section into the caller
                    if (err) { self.compacting = false; return callback(err) }
                    self.seekoffset -= self.position;
                    self.position = self.dataOffset;
                    self.compacting = false;
                    self.rsync(callback);
                })
            })
        })
    })
}

/*
 * copy the bytes from srcFd between srcOffset and srcLimit into dstFd to offset dstOffset
 * TODO: see if can combine this loop with _readsome and _writesome
 * nb: it is ok to read and write 0 bytes
 */
QFifo.prototype.copyBytes = function copyBytes( srcFd, dstFd, srcOffset, srcLimit, dstOffset, buff, callback ) {
    (function readWriteLoop() {
        var nchunk = Math.min(buff.length, srcLimit - srcOffset);
        fs.read(srcFd, buff, 0, nchunk, srcOffset, function(err, nread) {
            if (err) return callback(err);
            srcOffset += nread;
            fs.write(dstFd, buff, 0, nread, dstOffset, function(err, nwritten) {
                if (err) return callback(err);
                dstOffset += nwritten;
                if (nwritten < nchunk) callback(null, dstOffset);
                else readWriteLoop();
            })
        })
    })();
}

/*
 * remove the fifo.  Reads and writes are still possible until closed.
 */
QFifo.prototype.remove = function remove( callback ) {
    var self = this;
    fs.unlink(self.filename, function(err) {
        if (err) return callback(err);
        fs.unlink(self.headername, function(err2) {
            if (err2 && err2.code === 'ENOENT') err2 = null;
            callback(err2);
        })
    })
}

/*
 * rename the fifo filename to newName, and filename.hd to newName.hd
 * Keep the fd open, let reads and writes continue as before.
 */
QFifo.prototype.rename = function rename( newName, callback ) {
    var self = this;
    fs.rename(self.filename, newName, function(err) {
        if (err) return callback(err);
        // do not leave the previous .hd if fifo has none
        try { fs.unlinkSync(newName + '.hd') } catch (e) {}
        fs.rename(self.headername, newName + '.hd', function(err2) {
            if (err2 && err2.code !== 'ENOENT') return callback(err2);
            self.filename = newName;
            self.headername = newName + '.hd';
            callback();
        })
    })
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

var spaces200 =
    '                                                                                                    ' +
    '                                                                                                    ';
// 43 usec fs.writeFile 200B async, 7.3 usec all sync, 4.3 usec if sync opened 'r+', 1.5 us if fd already open,
// 1.1 us if smarter about json encoding
// nb: faster to poke spaces into buffer singly than to buf.write chunks of 40 bytes
QFifo.prototype.writeHeaderSync = function writeHeaderSync( fd, contents, buf, headerSize ) {
    var n = buf.write(contents);
    buf.write(spaces200, n, headerSize - n);
    buf[headerSize - 1] = CH_NL; // newline terminate the header json to separate it from any data lines
    return fs.writeSync(fd, buf, 0, headerSize, 0);
}
QFifo.prototype.readHeaderFile = function readHeaderFile( filename, readbuf, headerSize, callback ) {
    try { var fd = fs.openSync(filename, 'r') } catch (err) { return callback(err) }
    fs.read(fd, readbuf, 0, headerSize, 0, function(err, nread) {
        fs.closeSync(fd);
        callback(err, !err && readbuf.slice(0, nread));
    })
}
// pad an empty fifo with dataOffset blanks at the front to leave room for the in-file header
QFifo.prototype.skipDataOffset = function skipDataOffset( callback ) {
    var self = this;
    fs.stat(self.filename, function(err, stats) {
        if (err || stats.size > 0) return callback(err);
        self.readbuf.write(spaces200);
        // NOTE: max header size is 200 bytes
        fs.write(self.fd, self.readbuf, 0, self.dataOffset, null, callback);
    })
}

// aliases
QFifo.prototype.fflush = QFifo.prototype.flush;
QFifo.prototype.wsync = QFifo.prototype.flush;

QFifo.prototype = toStruct(QFifo.prototype);
function toStruct(hash) { return toStruct.prototype = hash }
