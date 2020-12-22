'use strict'

var fs = require('fs');
var QFifo = require('./');

var setImmediate = eval('global.setImmediate || process.nextTick');

module.exports = {
    beforeEach: function(done) {
        this.tempfile = '/tmp/test-qfifo-' + process.pid + '.txt';
        var rfifo = this.rfifo = new QFifo(this.tempfile, 'r');
        var wfifo = this.wfifo = new QFifo(this.tempfile, 'a');
        wfifo.open(function(err) {
            if (err) throw err;
            rfifo.open(done);
        })
    },

    afterEach: function(done) {
        this.rfifo.close();
        this.wfifo.close();
        fs.unlinkSync(this.tempfile);
        try { fs.unlinkSync(this.tempfile + '.hd') } catch (e) {}
        done();
    },

    'constructor': {
        'requires filename': function(t) {
            t.throws(function() { new QFifo() }, /filename/);
            t.done();
        },
        'rejects invalid open mode': function(t) {
            t.throws(function() { new QFifo('/nonesuch', 'w') }, /open mode/);
            t.done();
        },
    },

    'can open and read files': function(t) {
        var fifo = new QFifo(__filename);
        fifo.open(function(err, fd) {
            t.ifError(err);
            var line, lines = [];
            readall(fifo, new Array(), function(err, lines) {
                t.equal(lines.join(''), String(fs.readFileSync(__filename)));
                fifo.close();
                t.done();
            })
        })
    },

    'open / close': {
        'open reads the fifo header': function(t) {
            var fifo = new QFifo(this.tempfile);
            var spy = t.spyOnce(fs, 'readFile');
            fifo.open(function(err, fd) {
                t.ifError();
                t.ok(spy.called);
                t.equal(spy.args[0][0], fifo.headername);
                t.done();
            })
        },

        'open tolerates invalid json fifo header': function(t) {
            var fifo = new QFifo(__filename);
            var spy = t.stubOnce(fs, 'readFile').yields(null, '{]');
            fifo.open(function(err, fd) {
                t.ifError(err);
                t.equal(fifo.position, 0);
                fifo.close();
                t.done();
            })
        },

        'open tolerates null json fifo header': function(t) {
            var fifo = new QFifo(__filename);
            var spy = t.stubOnce(fs, 'readFile').yields(null, 'null');
            fifo.open(function(err, fd) {
                t.ifError(err);
                t.equal(fifo.position, 0);
                fifo.close();
                t.done();
            })
        },

        'open extracts read offset from header and applies it': function(t) {
            var fifo = new QFifo(__filename);
            var lines = fs.readFileSync(__filename).toString()
                .split('\n').slice(0, 10)
                .map(function(line) { return line + '\n' });
            var spy = t.stubOnce(fs, 'readFile').yields(null, '{"position":14}');
            fifo.open(function(err, fd) {
                t.ifError(err);
                t.equal(fifo.position, lines[0].length + lines[1].length);
                fifo.getline();
                setTimeout(function() {
                    // nb: several lines got buffered, can retrieve them synchronously
                    // we match the first 
                    t.equal(fifo.getline(), lines[2]);
                    t.equal(fifo.position, lines[0].length + lines[1].length + lines[2].length);
                    t.equal(fifo.getline(), lines[3]);
                    t.equal(fifo.position, lines[0].length + lines[1].length + lines[2].length + lines[3].length);
                    fifo.close();
                    t.done();
                }, 5);
            })
        },

        'return open error and sets fd to -1': function(t) {
            var fifo = new QFifo('/nonesuch');
            t.equal(fifo.fd, -1);
            fifo.open(function(err, fd) {
                t.equal(err.code, 'ENOENT');
                t.equal(fd, -1);
                t.equal(fifo.fd, -1);
                t.done();
            })
        },

        'second open is noop': function(t) {
            var fifo = this.rfifo;
            fifo.open(function(err, fd) {
                t.ok(fd >= 0);
                fifo.open(function(err, fd2) {
                    t.ok(fd2 >= 0);
                    t.equal(fd2, fd);
                    t.done();
                })
            })
        },

        'second close is noop': function(t) {
            var fifo = this.wfifo;
            fifo.close();
            fifo.close();
            fifo.close();
            t.done();
        },

        'close catches errors': function(t) {
            var fifo = this.rfifo;
            var spy = t.stubOnce(fs, 'closeSync').throws('mock-closeSync-error');
            fifo.open(function(err) {
                t.ifError(err);
                fifo.close();
                t.ok(spy.called);
                t.done();
            })
        },
    },

    'rsync / fflush': {
        'rsync checkpoints the fifo header': function(t) {
            var tempfile = this.tempfile;
            var rfifo = this.rfifo;
            fs.writeFileSync(tempfile, 'line1\nline22\nline333\n');
            rfifo.getline();
            setTimeout(function() {
                t.equal(rfifo.getline(), 'line1\n');
                t.equal(rfifo.getline(), 'line22\n');
                rfifo.rsync(function(err, ret) {
                    t.ifError(err);
                    t.contains(String(fs.readFileSync(tempfile + '.hd')), '"position":13');
                    t.done();
                })
            }, 5);
        },

        'fflush waits for all previous lines to be written': function(t) {
            var wfifo = this.wfifo;
            var tempfile = this.tempfile;
            wfifo.putline('line1');
            wfifo.putline('line22');
            wfifo.fflush(function(err) {
                t.ifError(err);
                t.equal(fs.readFileSync(tempfile) + '', 'line1\nline22\n');
                wfifo.putline('line333');
                wfifo.fflush(function(err) {
                    t.ifError(err);
                    t.equal(fs.readFileSync(tempfile) + '', 'line1\nline22\nline333\n');
                    t.done();
                })
            })
        },

        'fflush returns existing fifo error': function(t) {
            this.wfifo.error = 'mock-write-error';
            this.wfifo.fflush(function(err) {
                t.equal(err, 'mock-write-error');
                t.done();
            })
        },
    },

    'read / write': {
        'able to read and write': function(t) {
            var rfifo = this.rfifo, wfifo = this.wfifo;
            for (var i=1; i<=4; i++) wfifo.putline('line-' + i);
            wfifo.fflush(function(err) {
                t.ifError(err);
                var lines = [];
                var line = rfifo.getline();
                if (line) lines.push(line);
                setTimeout(function() {
                    while ((line = rfifo.getline())) lines.push(line);
                    t.deepEqual(lines, ['line-1\n', 'line-2\n', 'line-3\n', 'line-4\n']);
                    t.done();
                }, 50);
            })
        },

        'writes lines that arrive while writing': function(t) {
            var rfifo = this.rfifo;
            var wfifo = this.wfifo;
            wfifo.putline('line-1');
            process.nextTick(function() { wfifo.putline('line-2') });
            setTimeout(function() { wfifo.putline('line-3') }, 1);
            setTimeout(function() {
                readall(rfifo, new Array(), function(err, lines) {
                    t.ifError(err);
                    t.deepEqual(lines, ['line-1\n', 'line-2\n', 'line-3\n']);
                    t.done();
                })
            }, 10);
        },

        'can fflush an empty fifo': function(t) {
            this.wfifo.fflush(function(err) {
                t.ifError(err);
                t.done();
            })
        },
    },

    'speed': {
        'write 100k 200B lines': function(t) {
            var tempfile = this.tempfile;
            var fifo = new QFifo(tempfile, 'a');
            var line = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' +
                       'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' +
                       'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' +
                       'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n';
            fifo.open(function(err, fd) {
                t.ifError(err);
                console.time('AR: write 100k');
                for (var i=0; i<100000; i++) fifo.putline(line);
                fifo.fflush(function(err) {
                    console.timeEnd('AR: write 100k');
                    // about 4.3m lines / sec (single blocking burst, no yielding)
                    t.ifError(err);
                    fifo.close();

                    fifo = new QFifo(tempfile, 'r');
                    console.time('AR: read 100k');
                    fifo.open(function(err, fd) {
                        t.ifError(err);
                        readall(fifo, new Array(), function(err, lines) {
                            console.timeEnd('AR: read 100k');
                            t.ifError(err);
                            t.equal(lines.length, 100000);
                            t.done();
                            // 16k-256k buf about 2.2m lines/sec, 4096k buf 2.5m/s
                        })
                    })
                })
            })
        },
    },
};

function readall( fifo, lines, cb ) {
    var line;
    (function loop() {
        while ((line = fifo.getline())) lines.push(line);
        if (fifo.error) cb(fifo.error, lines);
        if (fifo.eof) cb(null, lines);
        else setImmediate(loop);
    })();
}
