qfifo
=====

Quick file-based fifo to buffer newline terminated string data.

## Features

- Simple `open` / `getline` / `putline` / `close` semantics.
- Synchronous calls, asynchronous file i/o.
- Efficient batched read and write operations.
- Very high throughput, hundreds of megabytes of line data per second.

## Limitations

- QFifos are inherently single-reader, the reader owns the fifo.
- Concurrent writes are not supported (file locking is missing from nodejs).
  This differs from the quicklib Quick_Fifo_File that QFifo was based on.


Api
----------------

### fifo = new QFifo( filename, accessmode )

Create a fifo for reading or appending the named file.  Mode must be `'r'` or `'a'` for
read or append mode access.  The same fifo cannot both read and write.  Creating a fifo
is a fast, the file is only accessed or created on `open()`.

### fifo.open( callback(err, fd) )

Open or create the file the fifo is using.  Currently the files being read must already exist,
files appended are created empty.  Returns the file descriptor used or the open error.

### fifo.close( )

Close the file descriptor.  The fifo will not be usable until it is reopened.  Closing the fifo
while still writing will produce a write error; use fflush() to avoid this.

### fifo.putline( line )

Append a line of text to the file.  All lines must be newline terminated; putline will supply
the newline if it is missing.  Putline is a non-blocking call that buffers the line to be
written and returns immediately.  Writing is is done asychronously in the background in batches.
The application must periodically yield the cpu for the writes to happen.  Any write errors are
saved in `fifo.error` and no further writes will be performed.

### fifo.write( string )

Internal fifo append method that does not mandate newlines.  Use with caution.

### fifo.fflush( callback )

Call callback once all currently buffered writes have completed.  New writes made after the
fflush call are not waited for.

### line = fifo.getline( )

Return the next unread line from the file or the empty string `''` if there are no new lines
available.  Reading is asynchronous, it is done in batches in the background, so lines may
become available later.  Always returns a utf8 string.  The `fifo.eof` flag is set to indicate
that zero bytes were read from the file.  Retrying the read may clear the eof flag.  Read errors
are saved in `fifo.error` and no more of the file is read.

    var readFile(filename, callback) {
        var contents = '';
        var fifo = new QFifo(filename, 'r');
        fifo.open(function(err) {
            if (err) callback(err);
            else readLines();
            function readLines() {
                for (var i=0; i<100; i++) contents += fifo.getline();
                if (fifo.error || fifo.eof) callback(fifo.error, contents);
                // yield the cpu so the fifo can read more of the file
                else setImmediate(readLines);
            }
        })
    }

### fifo.rsync( callback )

Checkpoint the current `fifo.position` byte offset of the next unread line so if the fifo is
reopened it can resume reading where it left off.  Saves the information to a separate file
named after fifo file but with a `'.hd'` extension added, eg `filename.hd`.

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


Changelog
----------------

- 0.1.0 - first tested version
