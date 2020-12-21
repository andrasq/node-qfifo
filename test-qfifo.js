'use strict'

var fs = require('fs');
var QFifo = require('./');

module.exports = {
    beforeEach: function(done) {
        this.tempfile = '/tmp/test-qfifo-' + process.pid + '.txt';
        var wfifo = this.wfifo = new QFifo(this.tempfile);
        var rfifo = this.rfifo = new QFifo(this.tempfile);
        wfifo.open('w', function(err) {
            if (err) throw err;
            rfifo.open('r', done);
        })
    },

    afterEach: function(done) {
        this.rfifo.close();
        this.wfifo.close();
        fs.unlinkSync(this.tempfile);
        done();
    },

    'can open and read files': function(t) {
        var fifo = new QFifo(__filename);
        fifo.open('r', function(err, fd) {
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
        'return open error and sets fd to -1': function(t) {
            var fifo = new QFifo('/nonesuch');
            t.equal(fifo.fd, -1);
            fifo.open('r', function(err, fd) {
                t.equal(err.code, 'ENOENT');
                t.equal(fd, -1);
                t.equal(fifo.fd, -1);
                t.done();
            })
        },

        'can second open is noop': function(t) {
            var fifo = this.rfifo;
            fifo.open('r', function(err, fd) {
                t.ok(fd >= 0);
                fifo.open('r', function(err, fd2) {
                    t.ok(fd2 >= 0);
                    t.equal(fd2, fd);
                    t.done();
                })
            })
        },

        'can second close is noop': function(t) {
            var fifo = this.wfifo;
            fifo.close();
            fifo.close();
            fifo.close();
            t.done();
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
            var fifo = new QFifo(this.tempfile);
            var line = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' +
                       'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' +
                       'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' +
                       'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n';
            fifo.open('a', function(err, fd) {
                t.ifError(err);
                console.time('AR: write 100k');
                for (var i=0; i<100000; i++) fifo.putline(line);
                fifo.fflush(function(err) {
                    console.timeEnd('AR: write 100k');
                    // about 4.3m lines / sec (single blocking burst, no yielding)
                    t.ifError(err);
                    fifo.close();

                    console.time('AR: read 100k');
                    fifo.open('r', function(err, fd) {
                        t.ifError(err);
                        readall(fifo, new Array(), function(err, lines) {
                            console.timeEnd('AR: read 100k');
                            t.ifError(err);
                            t.equal(lines.length, 100000);
                            t.done();
                            // 16k buf: 1311 ms for 100k, 64K 340ms ie 300k lines/sec
                            // 256k buf: 102ms (90ms if not gathering lines), 1m/s
                            // 512k buf: 60ms gathering; 1024k 40ms, 2048k 32ms, 4096k 28ms
                            // so up to 3.5 million lines / sec, depending on read chunk size
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
        if (fifo.eof) cb(null, lines);
        else setTimeout(loop);
    })();
}
