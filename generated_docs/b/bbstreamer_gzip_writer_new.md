# bbstreamer_gzip_writer_new

## Location
[src/bin/pg_basebackup/bbstreamer_gzip.c:79-125](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_gzip.c#L79-L125)

## Overview
Creates a new backup streamer that compresses data using gzip compression and writes it to a file, supporting both file path creation and existing file handle usage.

## Definition

```c
bbstreamer *
bbstreamer_gzip_writer_new(char *pathname, FILE *file,
						   pg_compress_specification *compress)
```
## Detailed Description
This function creates a bbstreamer instance specifically for gzip-compressed output during PostgreSQL base backup operations. The function handles two scenarios: creating a new compressed file from a pathname, or wrapping an existing file handle with gzip compression. It initializes the gzip compression parameters based on the provided compression specification and sets up the appropriate callback operations for content processing.

The function requires libz (zlib) support at compile time. If libz is not available, it will terminate with a fatal error. When working with an existing file handle, it uses dup() to avoid conflicts with libz's requirement to close the underlying file descriptor.

## Parameters / Member Variables
- `*pathname`: File path used for error reporting and optionally for file creation if file parameter is NULL
- `*file`: Existing FILE handle to wrap with gzip compression, or NULL to create a new file
- `*compress`: Compression specification containing compression level and other parameters
## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - [pstrdup](../p/pstrdup.md)
  - gzopen
  - gzdopen
  - gzsetparams
  - [get_gz_error](../g/get_gz_error.md)
  - [pg_fatal](../p/pg_fatal.md)
- Called from (representative examples):
  - [CreateBackupStreamer](../C/CreateBackupStreamer.md) (in pg_basebackup.c:1196)

## Notes and Other Information
- Requires HAVE_LIBZ compilation flag for zlib support
- Uses Z_DEFAULT_STRATEGY for compression strategy
- File creation mode is "wb" (write binary)
- When file parameter is provided, duplicates the file descriptor to avoid conflicts with gzip's close behavior
- Returns a bbstreamer pointer that should be used with the backup streaming infrastructure

## Simplified Source

```c
bbstreamer *
bbstreamer_gzip_writer_new(char *pathname, FILE *file,
                           pg_compress_specification *compress)
{
#ifdef HAVE_LIBZ
    bbstreamer_gzip_writer *streamer;

    // Allocate and initialize gzip writer structure
    streamer = palloc0(sizeof(bbstreamer_gzip_writer));
    *((const bbstreamer_ops **) &streamer->base.bbs_ops) = &bbstreamer_gzip_writer_ops;

    streamer->pathname = pstrdup(pathname);

    // Open gzip file - either create new or wrap existing file handle
    if (file == NULL) {
        streamer->gzfile = gzopen(pathname, "wb");
        if (streamer->gzfile == NULL)
            pg_fatal("could not create compressed file \"%s\": %m", pathname);
    } else {
        int fd = dup(fileno(file));
        if (fd < 0)
            pg_fatal("could not duplicate stdout: %m");

        streamer->gzfile = gzdopen(fd, "wb");
        if (streamer->gzfile == NULL)
            pg_fatal("could not open output file: %m");
    }

    // Set compression parameters
    if (gzsetparams(streamer->gzfile, compress->level, Z_DEFAULT_STRATEGY) != Z_OK)
        pg_fatal("could not set compression level %d: %s",
                 compress->level, get_gz_error(streamer->gzfile));

    return &streamer->base;
#else
    pg_fatal("this build does not support compression with %s", "gzip");
    return NULL;
#endif
}
```