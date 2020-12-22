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
        try { fs.unlinkSync(this.tempfile) } catch (e) {}
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

        'open returns error and sets fd to -1': function(t) {
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

        'fflush waits for all currently buffered lines to be written': function(t) {
            var wfifo = this.wfifo;
            var tempfile = this.tempfile;
            wfifo.putline('line1');
            wfifo.putline('line22');
            wfifo.fflush(function(err) {
                t.ifError(err);
                t.equal(fs.readFileSync(tempfile) + '', 'line1\nline22\nline333\n');
                wfifo.putline('line4444');
                wfifo.fflush(function(err) {
                    t.ifError(err);
                    t.equal(fs.readFileSync(tempfile) + '', 'line1\nline22\nline333\nline4444\n');
                    t.done();
                })
            })
            // a line appended here is swept by the writeDelay into the first write
            wfifo.putline('line333');
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

        'can read and write the same fifo': function(t) {
            var fifo = this.wfifo;

            // write somem data
            fifo.putline('line1\n');
            fifo.putline('line22\n');
            // data is buffered not written yet
            t.equal(fifo.getline(), '');
            t.equal(fifo.getline(), '');
            // flush to be notified when the write happend
            fifo.fflush(function(err) {
                t.ifError(err);
                // start the read
                fifo.getline();
                // wait for the read to complete
                setTimeout(function() {
                    // read back the written data
                    t.equal(fifo.getline(), 'line1\n');
                    t.equal(fifo.position, 6);
                    t.equal(fifo.getline(), 'line22\n');
                    t.equal(fifo.position, 13);
                    // end of data, read more to detect the eof
                    t.equal(fifo.getline(), '');
                    t.equal(fifo.reading, true);
                    // wait for the second read to complete
                    setTimeout(function() {
                        // second read should see the eof
                        t.equal(fifo.eof, true);
                        // write new data and wait for the write to complete
                        fifo.putline('line333');
                        setTimeout(function() {
                            // kick off a read again
                            t.equal(fifo.getline(), '');
                            setTimeout(function() {
                                // see the new data, and because there was data no more eof
                                t.equal(fifo.getline(), 'line333\n');
                                t.equal(fifo.eof, false);
                                t.done();
                            }, 5)
                        }, 5)
                    }, 5)
                }, 5)
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

    'helpers': {
        '_getmore': {
            'sets the `fifo.reading` flag as a mutex': function(t) {
                var fifo = this.rfifo;
                t.equal(fifo.reading, false);
                fifo._getmore();
                t.equal(fifo.reading, true);
                setTimeout(function() {
                    t.equal(fifo.reading, false);
                    t.equal(fifo.eof, true);
                    t.done();
                }, 5);
            },
            'sets eof if zero bytes read': function(t) {
                var fifo = this.rfifo;
                fifo.eof = false;
                var spy = t.stubOnce(fs, 'read').yields(null, 0);
                fifo._getmore();
                setTimeout(function() {
                    t.ok(spy.called);
                    t.equal(fifo.eof, true);
                    t.done();
                }, 5);
            },
            'sets fifo.error on read error': function(t) {
                var fifo = this.rfifo;
                var spy = t.stubOnce(fs, 'read').yields('mock-read-error');
                fifo._getmore();
                setTimeout(function() {
                    t.ok(spy.called);
                    t.equal(fifo.error, 'mock-read-error');
                    t.done();
                }, 5);
            },
            'does not read if fifo.error is set': function(t) {
                var fifo = this.rfifo;
                fifo.error = 'mock-read-error';
                var spy = t.spy(fs, 'read');
                fifo._getmore();
                t.equal(fifo.reading, false);
                setTimeout(function() {
                    spy.restore();
                    t.equal(spy.called, false);
                    t.done();
                }, 5);
            },
        },

        '_writesome': {
            'uses the `fifo.writing` flag as a mutex': function(t) {
                var fifo = this.wfifo;
                t.equal(fifo.writing, false);
                fifo._writesome();
                t.equal(fifo.writing, true);
                setTimeout(function() {
                    t.equal(fifo.writing, false);
                    t.done();
                }, 5);
            },
            'sets fifo.error on write error': function(t) {
                var fifo = this.wfifo;
                var spy = t.stubOnce(fs, 'write').yields('mock-write-error');
                fifo._writesome();
                setTimeout(function() {
                    t.ok(spy.called);
                    t.equal(fifo.error, 'mock-write-error');
                    t.done();
                }, 5);
            },
            'does not write if fifo.error is set': function(t) {
                var fifo = this.wfifo;
                fifo.error = 'mock-write-error';
                var spy = t.spy(fs, 'write');
                fifo._writesome();
                t.equal(fifo.writing, false);
                setTimeout(function() {
                    spy.restore();
                    t.equal(spy.called, false);
                    t.done();
                }, 5);
            },
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
                    // about 3.5m lines / sec (single blocking burst, no yielding) (was 4.3)
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

// iterateSteps adapted from minisql from miniq, originally from qrepeat and aflow
function runSteps(steps, callback) {
    var ix = 0;
    (function _loop(err, a1, a2) {
        if (err || ix >= steps.length) return callback(err, a1, a2);
        steps[ix++](_loop, a1, a2);
    })()
}
