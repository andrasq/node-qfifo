/*
 * Copyright (C) 2020-2022 Andras Radics
 * Licensed under the Apache License, Version 2.0
 */

'use strict'

var fs = require('fs');
var QFifo = require('./');

var setImmediate = eval('global.setImmediate || process.nextTick');
var fromBuf = (parseInt(process.versions.node) >= 6 && Buffer.from) ? Buffer.from : Buffer;

module.exports = {
    beforeEach: function(done) {
        this.tempfile = '/tmp/test-qfifo-' + process.pid + '.txt';
        this.rfifo = new QFifo(this.tempfile, 'r+');
        this.wfifo = new QFifo(this.tempfile, 'a+');
        // pre-create the file else read fifos error out on open
        fs.writeFileSync(this.tempfile, "");
        done();
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
            t.throws(function() { new QFifo('/nonesuch', { flag: 'w' }) }, /open mode/);
            t.done();
        },
        'rejects non-numeric options': function(t) {
            t.throws(function() { new QFifo(__filename, { flag: 600 }) }, /flag.*string/);
            t.throws(function() { new QFifo(__filename, { readSize: null }) }, /readSize.*number/);
            t.throws(function() { new QFifo(__filename, { readSize: '765' }) }, /readSize.*number/);
            t.throws(function() { new QFifo(__filename, { writeSize: null }) }, /writeSize.*number/);
            t.throws(function() { new QFifo(__filename, { writeDelay: null }) }, /writeDelay.*number/);
            t.done();
        },
    },

    'isEmpty tests whether data in fifo': function(t) {
        var fifo = this.wfifo;
        t.equal(fifo.isEmpty(), true);
        fifo.putline('line1\n');
        fifo.putline('line22\n');
        t.equal(fifo.isEmpty(), false);
        setTimeout(function() {
            fifo.getline();
            setTimeout(function() {
                t.equal(fifo.getline(), 'line1\n');
                t.equal(fifo.isEmpty(), false);
                t.equal(fifo.getline(), 'line22\n');
                t.equal(fifo.isEmpty(), true);
                t.equal(fifo.isEmpty(), true);
                t.done();
            }, 5);
        }, fifo.options.writeDelay + 3);
    },

    'can open and read files': function(t) {
        var fifo = new QFifo(__filename);
        fifo.dataOffset = 0;
        fifo.open(function(err, fd) {
            t.ifError(err);
            var line, lines = [];
            readall(fifo, new Array(), function(err, lines) {
                t.ifError(err);
                t.equal(lines.join(''), String(fs.readFileSync(__filename)));
                fifo.close();
                t.done();
            })
        })
    },

    'putline': {
        'can append fifos': function(t) {
            var fifo = new QFifo(this.tempfile, 'a');
            fifo.putline('line1'); // callback is optional
            fifo.putline('line2', function(err) {
                t.ifError(err);
                var lines = readFifoSync(fifo).toString().split('\n');
                t.deepEqual(lines, ['line1', 'line2', '']);
                t.done();
            })
        },

        'cannot write read-only fifos': function(t) {
            var fifo = new QFifo(this.tempfile, 'r');
            fifo.putline('line1', function(err) {
                t.ok(err);
                t.equal(err.code, 'EBADF'); // 'r' is read-only, cannot append
                t.done();
            })
        },

        'can append reopened read-mode files': function(t) {
            var fifo = new QFifo(this.tempfile, { flag: 'r+', headerName: this.tempfile });
            t.equal(fifo.dataOffset, QFifo.HEADER_SIZE);
            t.equal(fifo.writepos, QFifo.HEADER_SIZE);
            runSteps([
                function(next) { fifo.putline('line-1'); fifo.flush(next) },
                function(next) { fifo.close(next) },
                function(next) { fifo.putline('line-22\n'); fifo.flush(next) },
                function(next) { fifo.close(next) },
                function(next) { fifo.putline('line-333'); fifo.flush(next) },
                function(next) { fifo.flush(next) },
            ], function(err) {
                t.ifError(err);
                var lines = fs.readFileSync(fifo.filename).slice(fifo.dataOffset).toString();
                t.deepEqual(lines.split('\n'), ['line-1', 'line-22', 'line-333', '']);
                t.done();
            })
        }
    },

    'open': {
        'fifos auto-open on read': function(t) {
            var fifo = new QFifo(__filename, 'r');
            fifo.dataOffset = 0;
            fifo.getline();
            setTimeout(function() {
                t.equal(fifo.getline(), fs.readFileSync(__filename).toString().split('\n')[0] + '\n');
                t.done();
            }, 5)
        },

        'fifos auto-open on write': function(t) {
            var tempfile = this.tempfile;
            var fifo = new QFifo(tempfile, 'a');
            fifo.putline('line1');
            fifo.putline('line2');
            setTimeout(function() {
                t.equal(readFifoSync(fifo), 'line1\nline2\n');
                t.done();
            }, fifo.options.writeDelay + 3);
        },

        'pads the file to leave room for the header': function(t) {
            var fifo = new QFifo(this.tempfile, { flag: 'a', headerName: this.tempfile });
            t.equal(fifo.dataOffset, QFifo.HEADER_SIZE); // default headerSize
            fifo.open(function(err, fd) {
                t.ifError(err);
                fifo.putline('line123');
                setTimeout(function() {
                    var contents = fs.readFileSync(fifo.filename).toString();
                    t.equal(Buffer.byteLength(contents), fifo.dataOffset + 8);
                    t.done();
                }, fifo.options.writeDelay + 3);
            })
        },

        'open reads the fifo header': function(t) {
            var fifo = new QFifo(this.tempfile);
            fs.writeFileSync(fifo.headername, JSON.stringify({ skip: 100, position: 123, wpos: 234 }));
            var spy = t.spyOnce(fifo, 'readHeaderFile');
            fifo.open(function(err, fd) {
                t.ifError(err);
                t.ok(spy.called);
                t.equal(spy.args[0][0], fifo.headername);
                t.equal(fifo.dataOffset, 100);
                t.equal(fifo.position, 123);
                t.equal(fifo.writepos, 234);
                t.done();
            })
        },

        'open tolerates invalid json fifo header': function(t) {
            var fifo = new QFifo(__filename);
            var spy = t.stubOnce(fifo, 'readHeaderFile').yields(null, '{ invalid json }');
            fifo.open(function(err, fd) {
                t.ifError(err);
                t.equal(fifo.position, fifo.dataOffset);
                fifo.close();
                t.done();
            })
        },

        'open tolerates null json fifo header': function(t) {
            var fifo = new QFifo(__filename);
            var spy = t.stubOnce(fifo, 'readHeaderFile').yields(null, 'null');
            fifo.open(function(err, fd) {
                t.ifError(err);
                t.equal(fifo.position, fifo.dataOffset);
                fifo.close();
                t.done();
            })
        },

        'open extracts read offset from header and applies it': function(t) {
            var fifo = new QFifo(__filename, { headerName: this.tempfile + '.hd' });
            fifo.dataOffset = 0;
            var lines = fs.readFileSync(__filename).toString()
                .split('\n').slice(0, 10)
                .map(function(line) { return line + '\n' });
            var spy = t.stubOnce(fifo, 'readHeaderFile').yields(null, '{"position":' + (lines[0].length + lines[1].length) + '}');
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

        'open returns data file error and sets fd to -1': function(t) {
            var fifo = new QFifo('/nonesuch');
            t.equal(fifo.fd, -1);
            fifo.open(function(err, fd) {
                t.equal(err.code, 'ENOENT');
                t.equal(fd, -1);
                t.equal(fifo.fd, -1);
                t.done();
            })
        },

        'open returns header error and sets fd to -1': function(t) {
            var fifo = new QFifo(this.tempfile);
            t.stubOnce(fifo, 'readHeaderFile').yields('mock header error');
            fifo.open(function(err, fd) {
                t.equal(err, 'mock header error');
                t.equal(fifo.fd, -1);
                t.done();
            })
        },

        'open returns invalid header length': function(t) {
            var fifo = new QFifo(this.tempfile);
            t.stubOnce(fifo, 'readHeaderFile').yields(null, JSON.stringify({ skip: 300 }));
            fifo.open(function(err, fd) {
                t.ok(err)
                t.ok(/header too long/.test(err.message));
                t.equal(fifo.fd, -1);
                t.done();
            })
        },

        'open returns stat error and sets fd to -1': function(t) {
            var fifo = new QFifo(this.tempfile);
            t.stubOnce(fs, 'stat').yields('mock stat error');
            fifo.open(function(err, fd) {
                t.equal(err, 'mock stat error');
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

        'concurrent opens are allowed': function(t) {
            var fifo = this.rfifo;
            var fds = [];
            fifo.open(gatherFd);
            fifo.open(gatherFd);
            fifo.open(gatherFd);
            function gatherFd(err, fd) {
                t.ifError(err);
                fds.push(fd);
                if (fds.length === 3) {
                    t.equal(fds[0], fds[1]);
                    t.equal(fds[1], fds[2]);
                    t.done();
                }
            }
        },
    },

    'close': {
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

        'calls optional callback': function(t) {
            var fifo = this.rfifo;
            t.stubOnce(fs, 'closeSync', function() { throw 'mock error' });
            fifo.open(function() {
                fifo.close(function(err) {
                    t.equal(err, 'mock error');
                    t.done();
                })
            })
        },

        'closes fds': function(t) {
            var fifo = this.wfifo;
            var spy = t.spy(fs, 'closeSync');
            runSteps([
                function(next) {
                    // auto-open on first write
                    fifo.putline('line1');
                    fifo.putline('line2');
                    fifo.flush(next);
                },
                function(next) {
                    fifo.rsync(next);
                },
                function(next) {
                    t.ok(fifo.fd >= 0);
                    fifo.close(next);
                }
            ], function(err) {
                spy.restore();
                t.ifError(err);
                t.ok(spy.called);
                t.equal(fifo.fd, -1);
                t.done();
            })
        },
    },

    'rsync / fflush': {
        'rsync checkpoints the fifo header': function(t) {
            var tempfile = this.tempfile;
            var rfifo = this.rfifo;
            writeFifoSync(rfifo, 'line1\nline22\nline333\n');
            var now = Date.now();
            rfifo.getline();
            setTimeout(function() {
                t.equal(rfifo.getline(), 'line1\n');
                t.equal(rfifo.getline(), 'line22\n');
                rfifo.rsync(function(err, ret) {
                    t.ifError(err);
                    var header = JSON.parse(fs.readFileSync(tempfile + '.hd'));
                    t.contains(header, { position: rfifo.dataOffset + 13, wpos: rfifo.dataOffset + 21 });
                    t.ok(header.rtime >= now);
                    t.done();
                })
            }, 5);
        },

        'rsync returns write error': function(t) {
            t.stubOnce(fs, 'writeSync').throws('mock-write-error');
            this.rfifo.rsync(function(err) {
                t.equal(err, 'mock-write-error');
                t.done();
            })
        },

        'fflush waits for all currently buffered lines to be written': function(t) {
            var wfifo = this.wfifo;
            var tempfile = this.tempfile;
            wfifo.putline('line1');
            wfifo.putline('line22');
            wfifo.fflush(function(err) {
                t.ifError(err);
                t.equal(readFifoSync(wfifo) + '', 'line1\nline22\nline333\n');
                wfifo.putline('line4444');
                wfifo.fflush(function(err) {
                    t.ifError(err);
                    t.equal(readFifoSync(wfifo) + '', 'line1\nline22\nline333\nline4444\n');
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
                }, 5);
            })
        },

        'able to write non-strings': function(t) {
            var fifo = this.wfifo;
            fifo.putline('line1\n');
            fifo.putline(fromBuf('line2\n'));
            fifo.putline(123);
            fifo.putline({});
            fifo.putline(null);
            fifo.fflush(function(err) {
                t.ifError(err);
                fifo.getline();
                setTimeout(function() {
                    var line, lines = [];
                    while ((line = fifo.getline())) lines.push(line);
                    t.deepEqual(lines, ['line1\n', 'line2\n', '123\n', '[object Object]\n', 'null\n']);
                    t.done();
                }, 5)
            })
        },

        'can read split multi-char utf8 characters': {
            'split start char': function(t) {
                var fifo = new QFifo(this.tempfile, { reopenInterval: 20 });
                fifo.getline();
                setTimeout(function() {
                    t.stubOnce(fs, 'read', function(fd, buf, start, count, position, cb) {
                        buf[start] = 0x41; buf[start + 1] = 0xCF; cb(null, 2) });
                    fifo.getline();
                    setTimeout(function() {
                        t.stubOnce(fs, 'read', function(fd, buf, start, count, position, cb) {
                            buf[start] = 0x80; buf[start + 1] = 0x42; buf[start + 2] = 0x0A; cb(null, 3) });
                        fifo.getline();
                        setImmediate(function() {
                            t.equal(fifo.getline(), 'A\u03C0B\n');
                            t.done();
                        });
                    }, 5);
                }, 5);
            },

            'split last char': function(t) {
                var fifo = new QFifo(this.tempfile, { reopenInterval: 20 });
                fifo.getline();
                setTimeout(function() {
                    t.stubOnce(fs, 'read', function(fd, buf, start, count, position, cb) {
                        buf[start] = 0x41; buf[start + 1] = 0xCF; buf[start + 2] = 0x80; cb(null, 3) });
                    fifo.getline();
                    setTimeout(function() {
                        t.stubOnce(fs, 'read', function(fd, buf, start, count, position, cb) {
                            buf[start] = 0x42; buf[start + 1] = 0x0A; cb(null, 2) });
                        fifo.getline();
                        setImmediate(function() {
                            t.equal(fifo.getline(), 'A\u03C0B\n');
                            t.done();
                        });
                    }, 5);
                }, 5);
            },
        },

        'can read and write very long lines': function(t) {
            var str = new Array(1e6).join('x') + '\n';
            var fifo = this.wfifo;
            var lines = []; for (var i=0; i<20; i++) lines[i] = str;
            console.time('AR: write 1mb x20');
            writeall(fifo, lines, function(err) {
                console.timeEnd('AR: write 1mb x20');
                // 8ms to write 10mb
                t.ifError(err);
                console.time('AR: read 1mb x20');
                readall(fifo, [], function(err, readLines) {
                    console.timeEnd('AR: read 1mb x20');
                    // 68ms to read 10mb (chunk combining)
                    t.ifError(err);
                    t.deepEqual(readLines, lines);
                    t.done();
                })
            })
        },

        'can read with reopening': function(t) {
            var str = new Array(200e3).join('x\n');
            var fifo = new QFifo(this.tempfile, { flag: 'a+', reopenInterval: 1 });
            fifo.putline(str);
            fifo.wsync(function(err) {
                t.ifError(err);
                var spy = t.spy(fs, 'closeSync');
                readall(fifo, [], function(err, lines) {
                    spy.restore();
                    t.ifError(err);
                    t.equal(lines.length, 200e3 - 1);
                    t.equal(lines[0], 'x\n');
                    t.ok(spy.callCount > 1);
                    t.done();
                })
            })
        },

        'can read without reopening': function(t) {
            var str = new Array(200e3).join('x\n');
            var fifo = new QFifo(this.tempfile, { flag: 'a+', reopenInterval: -1 });
            fifo.putline(str);
            fifo.wsync(function(err) {
                t.ifError(err);
                var spy = t.spy(fs, 'closeSync');
                readall(fifo, [], function(err, lines) {
                    spy.restore();
                    t.ifError(err);
                    t.equal(lines.length, 200e3 - 1);
                    t.equal(lines[0], 'x\n');
                    t.equal(spy.callCount, 0);
                    t.done();
                })
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
                    t.equal(fifo.position, fifo.dataOffset + 6);
                    t.equal(fifo.getline(), 'line22\n');
                    t.equal(fifo.position, fifo.dataOffset + 13);
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
                                // note: fifo.eof will be set if readahead tried twice in the past 5ms
                                // t.equal(fifo.eof, false);
                                t.done();
                            }, 5)
                        }, fifo.options.writeDelay + 3)
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
            }, wfifo.options.writeDelay + 8);
        },

        'can fflush an empty fifo': function(t) {
            this.wfifo.fflush(function(err) {
                t.ifError(err);
                t.done();
            })
        },

        'does not update position if updatePosition:false': function(t) {
            var fifo = new QFifo(this.tempfile, { flag: 'a+', updatePosition: false });
            t.equal(fifo.position, fifo.dataOffset + 0);
            fifo.write('line1\nline22\nline333\n');
            fifo.fflush(function(err) {
                t.ifError(err);
                fifo.getline();
                setTimeout(function() {
                    t.equal(fifo.getline(), 'line1\n');
                    t.equal(fifo.position, fifo.dataOffset + 0);
                    t.equal(fifo.getline(), 'line22\n');
                    t.equal(fifo.position, fifo.dataOffset + 0);
                    t.equal(fifo.getline(), 'line333\n');
                    t.equal(fifo.position, fifo.dataOffset + 0);
                    t.equal(fifo.getline(), '');
                    t.equal(fifo.position, fifo.dataOffset + 0);
                    t.done();
                }, 5)
            })
        },
    },

    'readlines': {
        'reads file in chunks': function(t) {
            var fifo = new QFifo(__filename, { readSize: 20 });
            fifo.dataOffset = 0;
            var lines = '';
            fifo.readlines(function(line) {
                t.ok(line[line.length - 1] === '\n');
                lines += line;
                if (fifo.eof) {
                    t.equal(lines, fs.readFileSync(__filename).toString());
                    t.ok(fifo.eof);
                    t.done();
                }
            });
        },

        'does not deliver lines until resumed': function(t) {
            var fifo = new QFifo(__filename);
            fifo.dataOffset = 0;
            var now = Date.now();
            fifo.pause();
            fifo.readlines(function(line) {
                t.ok(Date.now() >= now + 20 - 1);
                fifo.pause();
                t.done();
            })
            setTimeout(function() { fifo.resume() }, 20);
        },

        'can pause/resume': function(t) {
            var fifo = new QFifo('package.json');
            fifo.dataOffset = 0;
            var lineTimes = [];
            fifo.readlines(function(line) {
                lineTimes.push(Date.now());
                fifo.pause();
                setTimeout(function() { fifo.resume() }, 2);
                if (fifo.eof) {
                    var contents = fs.readFileSync('package.json').toString();
                    t.equal(lineTimes.length, contents.split('\n').length - 1);
                    for (var i=1; i<lineTimes.length; i++) t.ok(lineTimes[i] > lineTimes[i-1]);
                    t.done();
                }
            })
        },

/***
        // NOTE: readlines stops once no more lines, it does not watch the fifo
        'delivers new lines as they appear': function(t) {
            var fifo = new QFifo(this.tempfile, 'a+');
            fifo.putline('line1');
            fifo.putline('line22');
            var lines = [];
            fifo.readlines(function(line) {
                lines.push(line);
                if (lines.length === 3) {
                    t.equal(lines[2], 'line333\n');
                    t.done();
                }
            })
            setTimeout(function() { fifo.putline('line333') }, 10);
        },
**/
    },

    'helpers': {
        '_readsome': {
            'sets the `fifo.reading` flag as a mutex': function(t) {
                var fifo = this.rfifo;
                t.equal(fifo.reading, false);
                fifo._readsome();
                t.equal(fifo.reading, true);
                setTimeout(function() {
                    t.equal(fifo.reading, false);
                    t.equal(fifo._eof, true);
                    t.done();
                }, 5);
            },
            'sets _eof if zero bytes read': function(t) {
                var fifo = this.rfifo;
                fifo._eof = false;
                var spy = t.stubOnce(fs, 'read').yields(null, 0);
                fifo._readsome();
                setTimeout(function() {
                    t.ok(spy.called);
                    t.equal(fifo._eof, true);
                    t.done();
                }, 5);
            },
            'sets fifo.error on read error': function(t) {
                var fifo = this.rfifo;
                var spy = t.stubOnce(fs, 'read').yields('mock-read-error');
                fifo._readsome();
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
                fifo._readsome();
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
                    t.equal(fifo.writing, false); // writing again false once write completed
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
                var spy = t.spyOnce(fs, 'write');
                fifo._writesome();
                t.equal(fifo.writing, false);
                setTimeout(function() {
                    t.equal(spy.called, false);
                    t.done();
                }, 5);
            },
        },

        'batchCalls': {
            'calls processBatchFunc with item list': function(t) {
                var uut = new QFifo('mockfile');
                var fn = uut.batchCalls(function processBatchFunc(list, cb) {
                    t.deepEqual(list, [1, 2, 3]);
                    t.done();
                })
                fn(1);
                fn(2);
                fn(3);
            },
            'invokes each callback': function(t) {
                var uut = new QFifo('mockfile');
                var fn = uut.batchCalls(function(list, cb) {
                    cb();
                })
                var cbs = [];
                fn(1, function(){ cbs.push(11) });
                fn(2, function(){ cbs.push(22) });
                fn(3, function(){ cbs.push(33); t.deepEqual(cbs, [11, 22, 33]); t.done() });
            },
            'waits maxWaitMs before processing': function(t) {
                var uut = new QFifo('mockfile');
                var startTime = Date.now();
                var fn = uut.batchCalls({ maxWaitMs: 7 }, function(list, cb) {
                    var now = Date.now();
                    t.ok(now >= startTime + 7 - 1);
                    cb();
                })
                fn(1, t.done);
            },
            'gathers at most maxBatchSize before processing': function(t) {
                var uut = new QFifo('mockfile');
                var expectBatches = [[1, 2], [3, 4], [5]];
                var fn = uut.batchCalls({ maxBatchSize: 2 }, function(list, cb) {
                    t.deepEqual(list, expectBatches.shift());
                    if (!expectBatches.length) t.done();
                })
                fn(1);
                fn(2);
                fn(3);
                fn(4);
                fn(5);
            },
            'uses custom startBatch and growBatch functions': function(t) {
                var uut = new QFifo('mockfile');
                var expectBatches = ['ab', 'c'];
                var fn = uut.batchCalls({
                    maxBatchSize: 2,
                    startBatch: function() { return '' },
                    growBatch: function(batch, item) { return batch + item },
                }, function(batch, cb) {
                    t.equal(batch, expectBatches.shift());
                    if (!expectBatches.length) t.done();
                })
                fn('a');
                fn('b');
                fn('c');
            },
        },

        'compact': {
            'compacts file': function(t) {
                var fifo = this.rfifo;
                writeFifoSync(fifo, 'line1\nline22\nline333\nline4444\n');
                runSteps([
                    function(next) {
                        fifo.getline();
                        setTimeout(next, 5);
                    },
                    function(next) {
                        t.equal(fifo.getline(), 'line1\n');
                        t.equal(fifo.getline(), 'line22\n');
                        fifo.rsync(next);
                    },
                    function(next) {
                        // will not compact if too small
                        fifo.compact(next);
                    },
                    function(next) {
                        t.equal(readFifoSync(fifo).toString(), 'line1\nline22\nline333\nline4444\n');
                        t.contains(JSON.parse(fs.readFileSync(fifo.headername).toString()), { position: fifo.dataOffset + 13 });
                        next();
                    },
                    function(next) {
                        // copy in tiny steps to also test chunk tiling
                        fifo.compact({ minSize: 0, minReadRatio: 0, readSize: 4 }, next);
                    },
                    function(next) {
                        t.equal(readFifoSync(fifo).toString(), 'line333\nline4444\n');
                        t.contains(JSON.parse(fs.readFileSync(fifo.headername).toString()), { position: fifo.dataOffset + 0 });
                        next();
                    },
                ], t.done);
            },

            'compacts an empty file': function(t) {
                var fifo = this.rfifo;
                var spy = t.spyOnce(fs, 'write');
                fifo.compact({ minSize: 0, minReadRatio: 0 }, function(err, dstOffset) {
                    t.ifError(err);
                    t.ok(spy.called);
                    t.equal(spy.args[0][0], fifo.fd);
                    t.equal(spy.args[0][3], 0);
                    t.done();
                })
            },

            'edge cases': {
                'returns open error': function(t) {
                    t.stubOnce(this.rfifo, 'open').yields('mock open error');
                    this.rfifo.compact({ minSize: 0, minReadRatio: 0 }, function(err) {
                        t.equal(err, 'mock open error');
                        t.done();
                    })
                },
                'returns copyBytes error': function(t) {
                    t.stubOnce(this.rfifo, 'copyBytes').yields('mock copy error');
                    this.rfifo.compact({ minSize: 0, minReadRatio: 0 }, function(err) {
                        t.equal(err, 'mock copy error');
                        t.done();
                    })
                },
                'returns ftruncate error': function(t) {
                    t.stubOnce(fs, fs.ftruncate ? 'ftruncate' : 'truncate').yields('mock ftruncate error');
                    this.rfifo.compact({ minSize: 0, minReadRatio: 0 }, function(err) {
                        t.equal(err, 'mock ftruncate error');
                        t.done();
                    })
                },
                'returns copyBytes read error': function(t) {
                    t.stubOnce(fs, 'read').yields('mock read error');
                    this.rfifo.compact({ minSize: 0, minReadRatio: 0 }, function(err) {
                        t.equal(err, 'mock read error');
                        t.done();
                    })
                },
                'returns copyBytes write error': function(t) {
                    t.stubOnce(fs, 'write').yields('mock write error');
                    this.rfifo.compact({ minSize: 0, minReadRatio: 0 }, function(err) {
                        t.equal(err, 'mock write error');
                        t.done();
                    })
                },
            },
        },

        'copyBytes': {
            // TODO: WRITEME
        },

        'remove': {
            'unlinks the fifo file': function(t) {
                var filename = this.wfifo.filename;
                writeFifoSync(this.wfifo, 'line1\n');
                this.wfifo.remove(function(err) {
                    fs.readFile(filename, function(err, bytes) {
                        t.equal(err && err.code, 'ENOENT');
                        t.done();
                    })
                })
            },
            'unlinks the header file too if it exists': function(t) {
                var headername = this.wfifo.headername;
                fs.writeFileSync(headername, '{}');
                this.wfifo.remove(function(err) {
                    fs.readFile(headername, function(err, bytes) {
                        t.equal(err && err.code, 'ENOENT');
                        t.done();
                    })
                })
            },
            'returns unlink errors': function(t) {
                var wfifo = this.wfifo;
                wfifo.remove(function(err) {
                    wfifo.remove(function(err) {
                        t.equal(err && err.code, 'ENOENT');
                        t.done();
                    })
                })
            },
            'returns header unlink errors': function(t) {
                this.wfifo.headername = '.';
                this.wfifo.remove(function(err) {
                    t.equal(err && err.code, 'EISDIR');
                    t.done();
                })
            },
        },

        'rename': {
            'renames the fifo': function(t) {
                var tempfile = this.tempfile;
                var fifo = this.wfifo;
                fifo.putline('line1');
                fifo.flush(function(err) {
                    t.ifError(err);
                    fifo.rename(fifo.filename + '.0', function(err) {
                        t.ifError(err);
                        t.equal(fifo.filename, tempfile + '.0');
                        t.equal(fifo.headername, tempfile + '.0.hd');
                        t.equal(readFifoSync(fifo).toString(), 'line1\n');
                        fs.unlinkSync(tempfile + '.0');
                        t.done();
                    })
                })
            },
            'renames the fifo and header': function(t) {
                var tempfile = this.tempfile;
                var fifo = this.rfifo;
                // sync to write header file
                fifo.rsync(function(err) {
                    fifo.rename(fifo.filename + '-new', function(err) {
                        t.ifError(err);
                        t.equal(fifo.filename, tempfile + '-new');
                        t.equal(fifo.headername, tempfile + '-new.hd');
                        // unlinks must succeed, file and header must exist by their new names
                        fs.unlinkSync(fifo.filename);
                        fs.unlinkSync(fifo.headername);
                        t.done();
                    })
                })
            },
            'returns error if unable to rename file': function(t) {
                var fifo = this.wfifo;
                fifo.remove(function() {
                    fifo.rename('/tmp/notfound', function(err) {
                        t.equal(err && err.code, 'ENOENT');
                        t.done();
                    })
                })
            },
            'returns error if unable to rename header': function(t) {
                this.wfifo.headername = '/home';
                this.wfifo.rename('/tmp/test-qfifo-new', function(err) {
                    // t.equal(err && err.code, 'EEXIST');  // getting EXDEV, but could also be EISDIR or EACCESS
                    fs.unlinkSync('/tmp/test-qfifo-new');
                    t.ok(err);
                    t.contains(err.message, /\/home/, "error message contains headername");
                    t.done();
                })
            },
        },

        'rotateFiles': {
            'renames all matching files': function(t) {
                var uut = new QFifo('fifoname');
                // mix up filename order to test the internal sorting
                t.stubOnce(fs, 'readdir').yields(null, ['fifoname.4', 'fifoname', 'fifoname.1', 'fifoname.3']);
                var spy = t.stub(fs, 'renameSync').configure('saveLimit', 10);
                uut.rotateFiles('fifoname', function(err, ret, names) {
                    spy.restore();
                    t.equal(spy.callCount, 4);
                    t.deepEqual(spy.args[0], ['fifoname.4', 'fifoname.5']);
                    t.deepEqual(spy.args[1], ['fifoname.3', 'fifoname.4']);
                    // missing file is not rotated
                    t.deepEqual(spy.args[2], ['fifoname.1', 'fifoname.2']);
                    // the original fifo file is rotated too
                    t.deepEqual(spy.args[3], ['fifoname', 'fifoname.1']);
                    t.deepEqual(names, ['fifoname.5', 'fifoname.4', 'fifoname.2', 'fifoname.1']);
                    t.done();
                })
            },

            'returns readdir errors': function(t) {
                var uut = new QFifo('fifoname');
                t.stubOnce(fs, 'readdir').yields('mock-readdir-error');
                uut.rotateFiles('fifoname', function(err, errors, names) {
                    t.equal(err, 'mock-readdir-error');
                    t.deepEqual(errors, ['mock-readdir-error']);
                    t.deepEqual(names, []);
                    t.done();
                })
            },

            'returns the rename errors and new filenames': function(t) {
                var uut = new QFifo('fifoname');
                t.stubOnce(fs, 'readdir').yields(null, ['fn.1', 'fn.2', 'fn.3']);
                var errors = ['mock-err1', 'mock-err2'];
                var spy = t.stub(fs, 'renameSync', function() { throw errors.shift() });
                uut.rotateFiles('fn', function(err, errors, names) {
                    spy.restore();
                    t.equal(spy.callCount, 3);
                    t.equal(err, 'mock-err1');
                    t.deepEqual(errors, ['mock-err1', 'mock-err2']);
                    // 3->4 fails, 2->3 fails 1-2 succeeds
                    t.deepEqual(names, ['fn.2']);
                    t.done();
                })
            },
        },
    },

    'end-to-end': {
        'supports arbitrary dataOffset': function(t) {
            var headerLine = '{"skip":80}                                                                    \n';
            var fifo = new QFifo(this.tempfile, { flag: 'a+', headerName: this.tempfile });
            fs.writeFileSync(this.tempfile, headerLine);
            runSteps([
                function(next) {
                    fifo.open(next);
                },
                function(next) {
                    t.equal(fifo.dataOffset, 80);
                    fifo.putline('line1');
                    fifo.putline('line22');
                    fifo.flush(next);
                },
                function(next) {
                    var lines = fs.readFileSync(fifo.headername).toString().trim().split('\n');
                    t.deepEqual(lines.slice(1), ['line1', 'line22']);
                    t.equal(lines[0], headerLine.slice(0, -1));
                    fifo.getline();
                    setTimeout(next, 5);
                },
                function(next) {
                    fifo.getline();
                    t.equal(fifo.position, fifo.dataOffset + 6);
                    fifo.rsync(next);
                },
                function(next) {
                    next();
                },
            ], function(err) {
                t.ifError(err);
                var now = Date.now();
                var lines = fs.readFileSync(fifo.headername).toString().trim().split('\n');
                t.deepEqual(lines.slice(1), ['line1', 'line22']);
                var header = JSON.parse(lines[0]);
                t.contains(header, { skip: 80, position: 80 + 6 });
                t.ok(header.rtime > now - 100 && header.rtime <= now);
                t.done();
            })
        },
    },

    'speed': {
        'write 1m lines': function(t) {
            var fifo = new QFifo(this.tempfile, 'a');
            var buf = fromBuf('x\n');
            var buflines = new Array(1000000); for (var i=0; i<buflines.length; i++) buflines[i] = buf;
            var strlines = new Array(1000000); for (var i=0; i<strlines.length; i++) strlines[i] = 'x\n';
            var mixlines = new Array(1000000); for (var i=0; i<strlines.length; i+=2) { mixlines[i] = 'x\n'; mixlines[i+1] = buf }
            runSteps([
                function(next) { console.time('AR: write 1m bufs'); next() },
                function(next) { writeall(fifo, buflines, next) },
                function(next) { console.timeEnd('AR: write 1m bufs'); next() },
                // 175ms / 1m buffers

                function(next) { console.time('AR: write 1m strs'); next() },
                function(next) { writeall(fifo, strlines, next) },
                function(next) { console.timeEnd('AR: write 1m strs'); next() },
                // 50ms / 1m strings (65ms/m standalone)

                function(next) { console.time('AR: write 1m mixs'); next() },
                function(next) { writeall(fifo, mixlines, next) },
                function(next) { console.timeEnd('AR: write 1m mixs'); next() },
                // 100ms / 1m mixed, ie no penalty for mix-and-match
            ], t.done);
        },

        'write 100k 200B lines, then read them': function(t) {
            var tempfile = this.tempfile;
            var fifo = new QFifo(tempfile, 'a');
            var line = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' +
                       'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' +
                       'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' +
                       'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n';

            // NOTE: map() and forEach() skip unset elements, but Array.from() can filter
            var lines = new Array(100000); for (var i=0; i<lines.length; i++) lines[i] = line;

            console.time('AR: write 200B x100k');
            writeall(fifo, lines, function(err) {
                console.timeEnd('AR: write 200B x100k');
                // about 3.2m lines / sec, 31ms if single 20mb burst
                // 4.6m/s, 21ms if yielding the cpu after every 100 lines
                // 5600x 4.9g: 6.0m/s, 16ms yielding every 200; 9.0m/s, 11ms every 50

                t.ifError(err);
                t.ifError(fifo.error);
                fifo.close();
                fifo = new QFifo(tempfile, { flag: 'r', updatePosition: true });

                console.time('AR: read 200B x100k');
                readall(fifo, null, function(err, lines) {
                    console.timeEnd('AR: read 200B x100k');

                    t.ifError(err);
                    t.ifError(fifo.error);
                    t.equal(lines.length, 100000);
                    t.done();
                    // 32k and larger buf about 2.5m lines/sec
                    // 5600x 4.9g: 5.9m/s, 16.5ms (8.5m/s 12ms w/o updatePosition)
                })
            })
        },

        'readlines 100k 200B lines': function(t) {
            var line = new Array(200).join('x') + '\n';
            var fifo = new QFifo(this.tempfile, { flag: 'a+', updatePosition: true });
            var lines  = new Array(100000); for (var i=0; i<lines.length; i++) lines[i] = line;
            writeall(fifo, lines, function(err) {
                t.ifError(err);
                var nlines = 0;
                var eofSeen = false;
                var lastLine;
                console.time("AR: readlines 200B x100k");
                fifo.readlines(function(line) {
                    lastLine = line;
                    nlines += 1;
                    if (fifo.eof) {
                        if (!eofSeen) {
                            console.timeEnd("AR: readlines 200B x100k");
                            // about 6.7m/s 200b lines (9.8/s w/o updatePosition)
                            setTimeout(function() {
                                t.equal(nlines, 100001);
                                t.equal(lastLine, 'one more line to test fs.watch\n');
                                t.done();
                            }, fifo.options.writeDelay + 8);
                            fifo.putline('one more line to test fs.watch\n');
                        }
                        eofSeen = true;
                    }
                })
            })
        },

        'rsync 100k': function(t) {
            fs.writeFileSync(this.tempfile, "");
            var fifo = new QFifo(this.tempfile, { flag: 'r+', headerName: this.tempfile });
            console.time('AR: rsync 100k');
            var ndone = 0;
            fifo.open(function() {
                for (var i=0; i<100000; i++) fifo.rsync(function(){
                    ndone += 1;
                    if (ndone >= 100000) {
                        console.timeEnd('AR: rsync 100k');
                        var contents = fs.readFileSync(fifo.headername).toString().split('\n')[0];
                        t.equal(contents.length, 199);
                        t.contains(JSON.parse(contents), { position: fifo.dataOffset, rtime: 0 });
                        t.done();
                    }
                })
            })
        },
    },
};

function readall( fifo, lines, cb ) {
    // much faster to not gather lines into an array
    lines = lines || { length: 0, push: function() { this.length += 1 } };
    var line;
    (function loop() {
        //for (var i=0; i<500; i++) (line = fifo.getline()) && lines.push(line);
        while ((line = fifo.getline())) lines.push(line);
        if (fifo.error) cb(fifo.error, lines);
        if (fifo.eof) cb(null, lines);
        else setImmediate(loop);
    })();
}

function writeall( fifo, lines, cb ) {
    var i = 0;
    (function loop() {
        // TODO: writing 200 200B lines with a 16k write buffer runs at 5m lines / sec
        // TODO: Try to capture this in the writeSize chunking logic
        for (var j=0; j<50; j++) if (i < lines.length) fifo.putline(lines[i++]);
        if (i >= lines.length) return fifo.fflush(cb);
        setImmediate(loop);
    })();
}

function writeFifoSync( fifo, lines ) {
    var offsetPad = new Array(fifo.dataOffset + 1).join(' ');
    fs.writeFileSync(fifo.filename, offsetPad + lines);
}

function readFifoSync( fifo ) {
    return fs.readFileSync(fifo.filename).slice(fifo.dataOffset).toString();
}

// iterateSteps adapted from minisql from miniq, originally from qrepeat and aflow
function runSteps(steps, callback) {
    var ix = 0;
    (function _loop(err, a1, a2) {
        if (err || ix >= steps.length) return callback(err, a1, a2);
        try { steps[ix++](_loop, a1, a2) } catch (err) { _loop(err) }
    })()
}
