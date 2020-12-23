qfifo
=====
[![Build Status](https://travis-ci.org/andrasq/node-qfifo.svg?branch=master)](https://travis-ci.org/andrasq/node-qfifo)
<!-- [![Coverage Status](https://coveralls.io/repos/github/andrasq/node-qfifo/badge.svg?branch=master)](https://coveralls.io/github/andrasq/node-qfifo?branch=master) -->

Quick file-based fifo to buffer newline terminated string data, designed to be a very low
overhead, very fast local journal that can both quickly persist large volumes of data and
efficiently feed the ingest process that consumes it.

## Highlights

- Simple `open` / `getline` / `putline` / `close` semantics.
- Synchronous calls, asynchronous file i/o.
- Efficient batched file reads and writes.
- Very high throughput, hundreds of megabytes of data per second.
- Universally supported, very easy, very fast newline terminated plaintext format.
- Metadata is optional, and is kept alongside in a separate file.  Only the consumer
  might need metadata.
- No external dependencies.
- Works with node-v0.6 and up.

## Limitations

- QFifos are inherently single-reader, the consuming process owns the fifo.
- Single-writer, concurrent writes are not supported. (File locking is missing from nodejs, so
  this incarnation differs from quicklib Quick_Fifo_File that it was based on.)


Api
----------------

### fifo = new QFifo( filename, options )

Create a fifo for reading or appending the named file.  Mode must be `'r'` or `'a'` to control
whether the file will be created if missing: `a` append mode can create the file, `r` read mode
requires the file to already exist.  All fifos can both read and write.  Creating a new QFifo is
a fast, it only allocates the object; the fifo must still be `open`-ed before use.

Options may contain the following settings:
- `flag`: open mode flag for `fs.open()`, default `'r'`.
- `readSize`: how many bytes to read at a time, default 32K.
- `writeSize`: TBD
- `writeDelay`: TBD

### fifo.open( callback(err, fd) )

Open the file for use by the fifo.  Returns the file descriptor used or the open error.
The fifo has no lines yet after the open, reading is started by the first `getline()`.
It is safe to open the fifo than once, the duplicate calls are harmless.  Note that
`getline` and `putline` will open the file if it hasn't been already.

### fifo.close( )

Close the file descriptor.  The fifo will not be usable until it is reopened.  Closing the fifo
while still writing will produce a write error; use fflush().

### fifo.putline( line )

Append a line of text to the file.  All lines must be newline terminated; putline will supply
the newline if it is missing.  Putline is a non-blocking call that buffers the data and returns
immediately.  Writing is is done in batches asychronously in the background.  The application
must periodically yield the cpu for the writes to happen.  Any write errors are saved in
`fifo.error` and no further writes will be performed.

### fifo.write( string )

Internal fifo append method that does not mandate newlines.  Use with caution.

### fifo.fflush( callback )

Invoke callback() once the currently buffered writes have completed.  New writes made after the
fflush call are not waited for (but may still have been included in the batch).

### line = fifo.getline( )

Return the next unread line from the file or the empty string `''` if there are no new lines
available.  Reading is asynchronous, it is done in batches in the background, so lines may
become available later.  Always returns a utf8 string.  The `fifo.eof` flag is set to indicate
that zero bytes were read from the file.  Retrying the read may clear the eof flag.  Read errors
are saved in `fifo.error` and stop the file being read.

    var readFile(filename, callback) {
        var fifo = new QFifo(filename, { flag: 'r', readSize: 256 * 1024 });
        var contents = '';
        (function readLines() {
            for (var i=0; i<100; i++) contents += fifo.getline();
            if (fifo.error || fifo.eof) callback(fifo.error, contents);
            // yield the cpu so the fifo can read more of the file
            else setTimeout(readLines);
        })();
    }

### fifo.rsync( callback )

Checkpoint the byte offset of the next unread line from `fifo.position` so if the fifo is
reopened it can resume reading where it left off.  The information is saved in JSON form to a
separate file named the same as the fifo `filename` but with `'.hd'` appended.

### fifo.position

Byte offset of the next unseen line in the input file.

### fifo.error

Read or write error that was encountered.  Either stops the fifo.

### fifo.eof

Set when zero bytes are read from the file, cleared otherwise.


See Also
----------------
- qfgets
- qfputs
- Quick_Fifo_File in quicklib


Todo
----------------

- `preopen` option
- make `r`-mode `open` run `_getmore` and wait for results before returning
- allow for streaming Buffers straight to file


Changelog
----------------

- 0.2.1 - constructor `options`, pass-through `options.flag`, auto-open on fifo read/write, faster rsync
- 0.1.0 - first version
