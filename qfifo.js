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

function QFifo( filename, mode ) {
    mode = mode || 'r';
    if (mode[0] !== 'r' && mode[0] !== 'a') throw new Error(mode + ": bad open mode, expected 'r' or 'a'");
    if (!filename) throw new Error('missing filename');

    this.filename = filename;
    this.headername = filename + '.hd';
    this.mode = mode === 'r' ? 'r' : 'a';
    this.eof = false;           // no data from last read
    this.error = null;          // read error, bad file
    this.position = 0;          // byte offset of next line to be read


    // TODO: move this clutter into a Reader and a Writer object
    this.decoder = new sd.StringDecoder();
    this.readbuf = allocBuf(32 * 1024);
    this.seekposition = this.position;
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

QFifo.prototype.open = function open( callback ) {
    var self = this;
    if (this.fd >= 0) return callback(null, this.fd);
    fs.open(this.filename, this.mode, function(err, fd) {
        self.fd = err ? -1 : fd;
        if (err) { self.error = err; self.eof = true; callback(err, self.fd); return }
        fs.readFile(self.headername, function(err2, header) {
            try { var header = JSON.parse(String(header)) || {} } catch (e) { var header = { position: 0 } }
            self.position = self.seekposition = header.position || 0;
            self.eof = false;
            self.error = null;
            callback(err, self.fd);
        })
    });
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

// tracking readoffset idea borrowed from qfgets
QFifo.prototype.getline = function getline( ) {
    this._getmore();
    var self = this, ix = this.readstring.indexOf('\n', this.readoffset);
    if (ix < 0) {
        if (this.readoffset > 0) this.readstring = this.readstring.slice(this.readoffset);
        this.readoffset = 0;
        return '';
    } else {
        var line = this.readstring.slice(this.readoffset, ix + 1);
        this.readoffset = ix + 1;
        // TODO: this is an inefficient but easy way to track the read offset
        // TODO: maybe find eoln() newlines in the buffer, remember line offets
        this.position += Buffer.byteLength(line);
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
    if (!this.reading && !this.error) {
        this.reading = true;
        fs.read(this.fd, this.readbuf, 0, this.readbuf.length, self.seekposition, function(err, nbytes) {
            if (err) { self.error = err; self.eof = true; return }
            self.eof = (nbytes === 0);
            self.seekposition += nbytes;
            self.reading = false;
            if (nbytes > 0) {
                self.readstring += self.decoder.write(self.readbuf.slice(0, nbytes));
                // TODO: maybe keep track of the line starting offsets, eoln(buf)
            }
        })
    }
}

QFifo.prototype._writesome = function _writesome( ) {
    if (!this.writing && !this.error) {
        var self = this;
        self.writing = true;
        setTimeout(function writeit() {
            var nchars = self.writestring.length;
            var buf = fromBuf(self.writestring);
            self.writestring = '';
            fs.write(self.fd, buf, 0, buf.length, null, function(err, nbytes) {
                if (err) self.error = err; // and continue, to error out all the pending callbacks
                self.writtenCount += nchars;
                while (self.writeCbs.length && (self.writeCbs[0].count <= self.writtenCount || self.error)) {
                    self.writeCbs.shift().cb(self.error);
                }
                self.writestring ? writeit() : self.writing = false; // keep writing if there is more
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

/**
function eoln( buf, pos ) {
    for (var i=pos; i<buf.length; i++) if (buf[i] === 10) return i;
    return -1;
}
**/
