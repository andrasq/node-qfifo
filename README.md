qfifo
=====
[![Build Status](https://travis-ci.org/andrasq/node-qfifo.svg?branch=master)](https://travis-ci.org/andrasq/node-qfifo)
<!-- [![Coverage Status](https://coveralls.io/repos/github/andrasq/node-qfifo/badge.svg?branch=master)](https://coveralls.io/github/andrasq/node-qfifo?branch=master) -->

Quick file-based fifo to buffer newline terminated strings, designed to be a very low
overhead, very fast local journal that can both quickly persist large volumes of data and
efficiently feed the ingest process that consumes it.

## Highlights

- Simple `open` / `getline` / `putline` / `close` semantics.
- Node `readline` emulation.
- Synchronous calls, asynchronous file i/o.
- Efficient batched file reads and writes.
- Very high throughput, hundreds of megabytes of data per second.
- Universally supported, very easy, very fast newline terminated plaintext data format.
- Metadata is optional, and is kept alongside in a separate file.  Only the consumer
  might need metadata.
- No external dependencies.
- Tested to work with node-v0.6 and up.

## Limitations

- QFifos are inherently single-reader, the consuming process owns the fifo.
- Single-writer; concurrent writes are not supported. (File locking is missing from nodejs, so
  this incarnation differs from the quicklib version it was based on.)


Api
----------------

### fifo = new QFifo( filename, options )

Create a fifo for reading or appending the named file.  Mode should be `'r+'` or `'a+'` to
control whether the file will be treated if missing: `r+` read mode to error out if the file
does not already exist, `a+` append mode to create the file if missing.  All fifos can both read
and write; for read-only or append-only access, omit the `+` from the flag.  Creating a new
QFifo is a fast, it only allocates the object; the fifo must still be `open`-ed (but note
that `getline`, `putline` and `readlines` automatically open the fifo).

Options may contain the following settings:
- `flag`: open mode flag for `fs.open()`, default `'r'`.
- `readSize`: how many bytes to read at a time, default 64K.
- `writeSize`: TBD
- `writeDelay`: TBD
- `updatePosition`: whether to update `fifo.position` with the byte offset in the file of the
  next line to be read.  Default `true`.  `position` is needed to checkpoint the read state, but
  omitting it is 25% faster.

### fifo.open( callback(err, fd) )

Open the file for use by the fifo.  Returns the file descriptor used or the open error.
The fifo has no lines yet after the open, reading is started by the first `getline()`.
It is safe to open the fifo more than once, the duplicate calls are harmless.  Note that
`getline` and `putline` will open the file if it hasn't been already.

### fifo.close( )

Close the file descriptor.  The fifo will not be usable until it is reopened.  Closing the fifo
while still writing will produce a write error; sync first with `flush()`.

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

### fifo.putline( line )

Append a line of text to the file.  All lines must be newline terminated; putline will supply
the newline if it is missing.  Putline is a non-blocking call that buffers the data and returns
immediately.  Writing is is done in batches asychronously in the background.  The application
must periodically yield the cpu for the writes to happen.  Any write errors are saved in
`fifo.error` and no further writes will be performed.

### fifo.write( string )

Internal fifo append method that does not mandate newlines.  Use with caution.

### fifo.flush( callback )

Invoke callback() once the currently buffered writes have completed.  New writes made after the
flush call are not waited for (but may still have been included in the batch).

### fifo.readlines( visitor(line) )

Loop getline() and call `visitor` with each line in the fifo.  `fifo.eof` will be set once the
fifo is empty and has no more lines.  If the fifo is appended, `eof` is cleared but the
readlines loop is not restarted.  Note that `readlines` and `getline` return lines that include
the terminating newline, which differs from node `readline` that strips them.

NOTE: This call is not reentrant, only a single function may be reading the fifo at a time.
Calling readlines() with a new `visitor` displaces the first function.

    fifo.readlines(function(line)) {
        // if (fifo.eof) then no more lines
    }

### fifo.pause( )

Suspend the `readlines` loop, do not deliver any more lines until resumed.

### fifo.resume( )

Resume the `readlines` loop, start delivering lines again.

### fifo.position

Byte offset of the next unseen line in the input file.

### fifo.error

Read or write error that was encountered.  Either stops the fifo.  Errors also set `fifo.eof` so
loops that check just `eof` still terminate once no more data is forthcoming.

### fifo.eof

Flag set when the fifo contains no more lines, ie when the end of the file has been reached and
no more lines are left in the buffer.  Appending lines to the fifo and retrying the read clears
the `eof` flag.


Todo
----------------

- `preopen` option
- make `r`-mode `open` run `_getmore` and wait for results before returning
- allow for streaming Buffers straight to file
- maybe queue pending writes and have _writesome() write the queued parts


Changelog
----------------

- 0.4.0 - `rotateFiles` helper, fledgeling `batchCalls` helper
- 0.3.0 - `readlines/pause/resume` methods, `updatePosition` option for faster reading, set `eof`
          only when no more lines available
- 0.2.2 - allow writing Buffers, space-pad header files
- 0.2.1 - constructor `options`, pass-through `options.flag`, auto-open on fifo read/write, faster rsync
- 0.1.0 - first version
